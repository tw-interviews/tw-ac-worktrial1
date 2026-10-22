package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sandbox-manager/internal/snapstore"
)

type note struct {
	ID        int64     `json:"id"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
}

type server struct {
	db               *pgxpool.Pool
	kube             dynamic.Interface
	core             kubernetes.Interface
	restConfig       *rest.Config
	sandboxNamespace string
	// volumeRetention is how long a gone sandbox's snapshot (and record) is
	// kept so its files can still be downloaded. VOLUME_RETENTION, default 12h.
	volumeRetention time.Duration
	// store keeps workspace snapshots in S3; agent does file I/O on live
	// workspaces through each node's sandbox-agent. See snapshots.go, agent.go.
	store       *snapstore.Store
	agent       *agentClient
	restoreKick chan struct{}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, err := connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS notes (
		id         BIGSERIAL PRIMARY KEY,
		text       TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS sandboxes (
		sandbox_id UUID PRIMARY KEY,
		name       TEXT NOT NULL UNIQUE,
		namespace  TEXT NOT NULL,
		definition TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	// Nullable: sandboxes created before TTLs existed have neither, and never expire.
	if _, err := db.Exec(ctx, `ALTER TABLE sandboxes
		ADD COLUMN IF NOT EXISTS ttl_seconds INTEGER,
		ADD COLUMN IF NOT EXISTS expires_at  TIMESTAMPTZ`); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	// Volumes: NULL volume_name for sandboxes created before every sandbox had
	// one. deleted_at marks an API delete; the row stays for the recovery window.
	if _, err := db.Exec(ctx, `ALTER TABLE sandboxes
		ADD COLUMN IF NOT EXISTS volume_name TEXT,
		ADD COLUMN IF NOT EXISTS volume_size TEXT,
		ADD COLUMN IF NOT EXISTS deleted_at  TIMESTAMPTZ,
		ADD COLUMN IF NOT EXISTS stage_files BOOLEAN NOT NULL DEFAULT false,
		ADD COLUMN IF NOT EXISTS staged_at   TIMESTAMPTZ`); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	// Pausing. operating_mode mirrors the Sandbox's spec.operatingMode as set
	// through the API. NULL paused_ttl_seconds: created before pausing
	// existed, and can't be paused.
	if _, err := db.Exec(ctx, `ALTER TABLE sandboxes
		ADD COLUMN IF NOT EXISTS operating_mode     TEXT NOT NULL DEFAULT 'Running',
		ADD COLUMN IF NOT EXISTS paused_ttl_seconds INTEGER,
		ADD COLUMN IF NOT EXISTS paused_at          TIMESTAMPTZ,
		ADD COLUMN IF NOT EXISTS resumed_at         TIMESTAMPTZ`); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	// Snapshots of /workspace (see snapshots.go).
	if _, err := db.Exec(ctx, `ALTER TABLE sandboxes
		ADD COLUMN IF NOT EXISTS snapshot_key   TEXT,
		ADD COLUMN IF NOT EXISTS snapshot_bytes BIGINT,
		ADD COLUMN IF NOT EXISTS snapshot_at    TIMESTAMPTZ`); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	// A sandbox has many executions; deleting the sandbox deletes its history.
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS executions (
		execution_id     UUID PRIMARY KEY,
		sandbox_id       UUID NOT NULL REFERENCES sandboxes (sandbox_id) ON DELETE CASCADE,
		pod              TEXT NOT NULL,
		command          TEXT[] NOT NULL,
		status           TEXT NOT NULL,
		exit_code        INTEGER,
		stdout           TEXT NOT NULL DEFAULT '',
		stderr           TEXT NOT NULL DEFAULT '',
		stdout_truncated BOOLEAN NOT NULL DEFAULT false,
		stderr_truncated BOOLEAN NOT NULL DEFAULT false,
		started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
		finished_at      TIMESTAMPTZ
	);
	CREATE INDEX IF NOT EXISTS executions_sandbox_id ON executions (sandbox_id, started_at)`); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	kube, err := newKubeClients()
	if err != nil {
		log.Fatalf("kubernetes client: %v", err)
	}

	retention, err := time.ParseDuration(envOr("VOLUME_RETENTION", "12h"))
	if err != nil {
		log.Fatalf("VOLUME_RETENTION: %v", err)
	}

	store, err := snapstore.FromEnv(ctx)
	if err != nil {
		log.Fatalf("snapshot store: %v", err)
	}
	agentToken := os.Getenv("AGENT_TOKEN")
	if agentToken == "" {
		log.Fatal("AGENT_TOKEN is required")
	}

	s := &server{
		db:               db,
		kube:             kube.dynamic,
		core:             kube.core,
		restConfig:       kube.config,
		sandboxNamespace: kube.namespace,
		volumeRetention:  retention,
		store:            store,
		agent:            newAgentClient(kube.core, envOr("AGENT_NAMESPACE", "webapp"), envOr("AGENT_PORT", "8090"), agentToken),
		restoreKick:      make(chan struct{}, 1),
	}
	go s.runGC(ctx, time.Minute)
	go s.runSnapshots(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /notes", s.listNotes)
	mux.HandleFunc("POST /notes", s.createNote)
	mux.HandleFunc("GET /notes/{id}", s.getNote)
	mux.HandleFunc("DELETE /notes/{id}", s.deleteNote)
	mux.HandleFunc("POST /sandboxes", s.createSandbox)
	mux.HandleFunc("GET /sandboxes", s.listSandboxes)
	mux.HandleFunc("GET /sandboxes/{id}", s.getSandbox)
	mux.HandleFunc("DELETE /sandboxes/{id}", s.deleteSandbox)
	mux.HandleFunc("POST /sandboxes/{id}/pause", s.pauseSandbox)
	mux.HandleFunc("POST /sandboxes/{id}/resume", s.resumeSandbox)
	mux.HandleFunc("POST /sandbox/executions", s.createExecution)
	mux.HandleFunc("GET /sandbox/executions", s.listExecutions)
	mux.HandleFunc("GET /sandbox/executions/{id}", s.getExecution)
	mux.HandleFunc("POST /sandboxes/{id}/files", s.uploadTar)
	mux.HandleFunc("GET /sandboxes/{id}/files", s.getFile)
	mux.HandleFunc("GET /sandboxes/{id}/files/{path...}", s.getFile)
	mux.HandleFunc("PUT /sandboxes/{id}/files/{path...}", s.putFile)
	mux.HandleFunc("DELETE /sandboxes/{id}/files/{path...}", s.deleteFile)
	mux.HandleFunc("POST /sandboxes/{id}/staging/complete", s.completeStaging)

	addr := ":" + envOr("PORT", "8080")
	srv := &http.Server{Addr: addr, Handler: logRequests(mux), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// connect retries until the database accepts connections, since the app pod
// may start before Postgres is ready.
func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := db.Ping(pingCtx)
		cancel()
		if err == nil {
			return db, nil
		}
		log.Printf("waiting for database: %v", err)
		select {
		case <-ctx.Done():
			db.Close()
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *server) healthz(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) listNotes(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `SELECT id, text, created_at FROM notes ORDER BY id`)
	if err != nil {
		s.internalError(w, err)
		return
	}
	notes, err := pgx.CollectRows(rows, pgx.RowToStructByPos[note])
	if err != nil {
		s.internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, notes)
}

func (s *server) createNote(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil || in.Text == "" {
		writeError(w, http.StatusBadRequest, `body must be JSON like {"text": "..."}`)
		return
	}
	var n note
	err := s.db.QueryRow(r.Context(),
		`INSERT INTO notes (text) VALUES ($1) RETURNING id, text, created_at`, in.Text,
	).Scan(&n.ID, &n.Text, &n.CreatedAt)
	if err != nil {
		s.internalError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, n)
}

func (s *server) getNote(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var n note
	err := s.db.QueryRow(r.Context(),
		`SELECT id, text, created_at FROM notes WHERE id = $1`, id,
	).Scan(&n.ID, &n.Text, &n.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "note not found")
		return
	}
	if err != nil {
		s.internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *server) deleteNote(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	tag, err := s.db.Exec(r.Context(), `DELETE FROM notes WHERE id = $1`, id)
	if err != nil {
		s.internalError(w, err)
		return
	}
	if tag.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "note not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) internalError(w http.ResponseWriter, err error) {
	log.Printf("error: %v", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be an integer")
		return 0, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeErrorCode is writeError plus a machine-readable code for clients to
// match on (e.g. "sandbox_paused").
func writeErrorCode(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"error": msg, "code": code})
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

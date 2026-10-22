// sandbox-agent runs on every node (DaemonSet) and does all file I/O on the
// workspaces of the sandbox pods on that node, on behalf of the
// SandboxManager API: live file reads and writes, snapshots to S3 (pause,
// delete, expiry) and restores from S3 (resume).
//
// A sandbox's workspace is the pod's emptyDir volume named
// "sandbox-workspace", which the kubelet keeps at
// <PODS_DIR>/<pod uid>/volumes/kubernetes.io~empty-dir/sandbox-workspace;
// the agent mounts the kubelet's pods directory from the host to reach it.
//
// HARDENING: that host directory holds every pod's volumes on the node,
// including other pods' Secrets and service account tokens, so a bug here
// can leak more than sandbox files. Keep this program small: every
// workspace path goes through an os.Root (internal/workspace), callers must
// present AGENT_TOKEN, pod UIDs are parsed as UUIDs, and only the two
// sandbox volume names are ever opened.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"

	"sandbox-manager/internal/snapstore"
	"sandbox-manager/internal/workspace"
)

const (
	workspaceVolume = "sandbox-workspace"
	controlVolume   = "sandbox-control"
	// readyMarker, in the control volume, releases the pod's wait init
	// container so the user container can start.
	readyMarker    = "ready"
	maxUploadBytes = 10 << 30
)

type agent struct {
	podsDir string
	token   []byte
	store   *snapstore.Store
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	token := os.Getenv("AGENT_TOKEN")
	if len(token) < 16 {
		log.Fatal("AGENT_TOKEN (at least 16 characters) is required")
	}
	store, err := snapstore.FromEnv(ctx)
	if err != nil {
		log.Fatalf("snapshot store: %v", err)
	}
	a := &agent{podsDir: envOr("PODS_DIR", "/var/lib/kubelet/pods"), token: []byte(token), store: store}

	// Files written for clients get exactly the modes asked for (0666 files,
	// 0777 directories by default), as the API has always produced.
	syscall.Umask(0)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /pods/{uid}/files", a.auth(a.get))
	mux.HandleFunc("GET /pods/{uid}/files/{path...}", a.auth(a.get))
	mux.HandleFunc("PUT /pods/{uid}/files/{path...}", a.auth(a.put))
	mux.HandleFunc("POST /pods/{uid}/files", a.auth(a.upload))
	mux.HandleFunc("DELETE /pods/{uid}/files/{path...}", a.auth(a.remove))
	mux.HandleFunc("POST /pods/{uid}/snapshot", a.auth(a.snapshot))
	mux.HandleFunc("POST /pods/{uid}/restore", a.auth(a.restore))
	mux.HandleFunc("POST /pods/{uid}/ready", a.auth(a.ready))

	addr := ":" + envOr("PORT", "8090")
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("listening on %s, pods in %s", addr, a.podsDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
}

// auth checks the bearer token, and logs the request.
func (a *agent) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		got := []byte(r.Header.Get("Authorization"))
		want := append([]byte("Bearer "), a.token...)
		if subtle.ConstantTimeCompare(got, want) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	}
}

// volume opens one of a pod's sandbox volumes, or writes 404 (code
// no_workspace) if the pod isn't on this node or has no such volume.
func (a *agent) volume(w http.ResponseWriter, r *http.Request, name string) (*os.Root, bool) {
	uid, err := uuid.Parse(r.PathValue("uid"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "pod uid must be a UUID")
		return nil, false
	}
	root, err := os.OpenRoot(filepath.Join(a.podsDir, uid.String(), "volumes", "kubernetes.io~empty-dir", name))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no sandbox workspace for this pod on this node", "code": "no_workspace"})
		return nil, false
	}
	return root, true
}

func (a *agent) get(w http.ResponseWriter, r *http.Request) {
	root, ok := a.volume(w, r, workspaceVolume)
	if !ok {
		return
	}
	defer root.Close()
	rel := workspace.Rel(r.PathValue("path"))
	if _, list := r.URL.Query()["list"]; list {
		entries, err := workspace.List(root, rel)
		if err != nil {
			writeFileError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, entries)
		return
	}
	fi, err := workspace.Stat(root, rel)
	if err != nil {
		writeFileError(w, err)
		return
	}
	if fi.IsDir() {
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(http.StatusOK)
		if err := workspace.Archive(root, rel, w); err != nil {
			log.Printf("archive %s: %v", r.URL.Path, err)
			panic(http.ErrAbortHandler) // headers are sent; cut the response short
		}
		return
	}
	f, size, err := workspace.Open(root, rel)
	if err != nil {
		writeFileError(w, err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	io.Copy(w, io.LimitReader(f, size))
}

func (a *agent) put(w http.ResponseWriter, r *http.Request) {
	root, ok := a.volume(w, r, workspaceVolume)
	if !ok {
		return
	}
	defer root.Close()
	rel := workspace.Rel(r.PathValue("path"))
	n, sum, err := workspace.Put(root, rel, http.MaxBytesReader(w, r.Body, maxUploadBytes))
	if err != nil {
		writeFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": "/workspace/" + rel, "bytes": n, "sha256": sum})
}

func (a *agent) upload(w http.ResponseWriter, r *http.Request) {
	root, ok := a.volume(w, r, workspaceVolume)
	if !ok {
		return
	}
	defer root.Close()
	files, n, err := workspace.Extract(root, "", http.MaxBytesReader(w, r.Body, maxUploadBytes), workspace.Upload)
	if err != nil {
		writeFileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"files": files, "bytes": n})
}

func (a *agent) remove(w http.ResponseWriter, r *http.Request) {
	root, ok := a.volume(w, r, workspaceVolume)
	if !ok {
		return
	}
	defer root.Close()
	rel := workspace.Rel(r.PathValue("path"))
	if rel == "" {
		writeError(w, http.StatusBadRequest, "refusing to delete the whole workspace")
		return
	}
	if err := workspace.Remove(root, rel); err != nil {
		writeFileError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// snapshot archives the whole workspace to a new S3 object for the sandbox:
// body {"sandbox_id"}, response {"key", "bytes"}. Nothing is stored if
// archiving fails.
func (a *agent) snapshot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SandboxID string `json:"sandbox_id"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil || uuid.Validate(req.SandboxID) != nil {
		writeError(w, http.StatusBadRequest, `body must be {"sandbox_id": "<uuid>"}`)
		return
	}
	root, ok := a.volume(w, r, workspaceVolume)
	if !ok {
		return
	}
	defer root.Close()
	key := a.store.NewKey(req.SandboxID)
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(workspace.Archive(root, "", pw)) }()
	n, err := a.store.Put(r.Context(), key, pr)
	pr.CloseWithError(io.ErrClosedPipe)
	if err != nil {
		log.Printf("snapshot %s: %v", req.SandboxID, err)
		writeError(w, http.StatusBadGateway, "snapshot failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "bytes": n})
}

// restore extracts a snapshot into the workspace (body {"key"}; an empty key
// restores nothing) and then marks the pod ready, which starts its user
// container.
func (a *agent) restore(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key string `json:"key"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		writeError(w, http.StatusBadRequest, `body must be {"key": "..."}`)
		return
	}
	root, ok := a.volume(w, r, workspaceVolume)
	if !ok {
		return
	}
	defer root.Close()
	var files, n int64
	if req.Key != "" {
		body, err := a.store.Get(r.Context(), req.Key)
		if err != nil {
			writeError(w, http.StatusBadGateway, "read snapshot: "+err.Error())
			return
		}
		defer body.Close()
		files, n, err = workspace.Extract(root, "", body, workspace.Restore)
		if err != nil {
			log.Printf("restore %s: %v", req.Key, err)
			writeFileError(w, err)
			return
		}
	}
	if !a.markReady(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"files": files, "bytes": n})
}

// ready marks the pod ready without restoring anything (staging complete).
func (a *agent) ready(w http.ResponseWriter, r *http.Request) {
	if a.markReady(w, r) {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *agent) markReady(w http.ResponseWriter, r *http.Request) bool {
	ctl, ok := a.volume(w, r, controlVolume)
	if !ok {
		return false
	}
	defer ctl.Close()
	if err := ctl.WriteFile(readyMarker, nil, 0o644); err != nil {
		writeError(w, http.StatusInternalServerError, "mark ready: "+err.Error())
		return false
	}
	return true
}

func writeFileError(w http.ResponseWriter, err error) {
	var bad *workspace.BadEntryError
	var tooBig *http.MaxBytesError
	switch {
	case errors.Is(err, workspace.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, workspace.ErrNotDir), errors.Is(err, workspace.ErrEscapes), errors.As(err, &bad):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, workspace.ErrIsDir), errors.Is(err, workspace.ErrSymlink):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, workspace.ErrNoSpace):
		writeError(w, http.StatusInsufficientStorage, "sandbox workspace is full")
	case errors.As(err, &tooBig):
		writeError(w, http.StatusRequestEntityTooLarge, "upload too large")
	default:
		log.Printf("file operation: %v", err)
		writeError(w, http.StatusBadRequest, fmt.Sprintf("file operation failed: %v", err))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

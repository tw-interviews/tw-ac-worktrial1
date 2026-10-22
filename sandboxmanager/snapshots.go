package main

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sandbox-manager/internal/workspace"
)

// Snapshots: a sandbox's workspace saved to S3 as a tar (internal/snapstore).
// The latest one's key is sandboxes.snapshot_key; every snapshot gets a new
// key and the row is switched to it only once it's fully stored, so a
// failure never loses the previous one. Replaced snapshots are deleted
// right after; leftovers are removed with the sandbox (DeleteAll).
//
// Snapshots are taken (by the node agent) when a sandbox is paused, deleted
// (without purge), or about to expire (snapshotExpiring); anything written
// after the last one is lost if the pod dies. A pod that starts with a
// snapshot to restore waits in its wait init container until restorePending
// has the agent restore it, so a resumed sandbox, or one whose pod was
// replaced, comes back with its last snapshot.

// expirySnapshotLead is how long before expires_at a running sandbox's
// workspace is saved, since the controller deletes it at that moment.
const expirySnapshotLead = time.Minute

// snapshotLive has the agent snapshot the sandbox's running pod. took=false
// (and no error) if there's nothing new to save: no pod, or one that hasn't
// finished initializing (its staging or restore isn't done).
func (s *server) snapshotLive(ctx context.Context, sb sandbox) (key string, size int64, took bool, err error) {
	obj, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Get(ctx, sb.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, err
	}
	pod, err := s.sandboxPodOf(ctx, sb, obj)
	if err != nil || pod == nil || !initialized(pod) {
		return "", 0, false, err
	}
	var out struct {
		Key   string `json:"key"`
		Bytes int64  `json:"bytes"`
	}
	start := time.Now()
	if err := s.agent.call(ctx, pod, http.MethodPost, "/snapshot", map[string]string{"sandbox_id": sb.SandboxID}, &out); err != nil {
		return "", 0, false, err
	}
	log.Printf("snapshot of sandbox %s: %d bytes in %s", sb.SandboxID, out.Bytes, time.Since(start).Round(time.Millisecond))
	return out.Key, out.Bytes, true, nil
}

// recordSnapshot makes key the sandbox's latest snapshot (in tx) and returns
// the key it replaces ("" if none), to delete once tx commits.
func recordSnapshot(ctx context.Context, tx pgx.Tx, sb *sandbox, key string, size int64) (string, error) {
	old := ""
	if sb.snapshotKey != nil {
		old = *sb.snapshotKey
	}
	err := tx.QueryRow(ctx,
		`UPDATE sandboxes SET snapshot_key = $2, snapshot_bytes = $3, snapshot_at = now() WHERE sandbox_id = $1 RETURNING snapshot_at`,
		sb.SandboxID, key, size).Scan(&sb.SnapshotAt)
	if err != nil {
		return "", err
	}
	sb.snapshotKey, sb.SnapshotBytes = &key, &size
	return old, nil
}

// dropSnapshot deletes a replaced snapshot (after the change is committed).
func (s *server) dropSnapshot(ctx context.Context, key string) {
	if key == "" {
		return
	}
	if err := s.store.Delete(context.WithoutCancel(ctx), key); err != nil {
		log.Printf("delete old snapshot %s: %v", key, err)
	}
}

// rewriteSnapshot replaces a paused sandbox's snapshot with a copy edited by
// workspace.Rewrite (drop, add), recording it in tx. Returns the replaced
// key, to drop after commit. On error nothing changes.
func (s *server) rewriteSnapshot(ctx context.Context, tx pgx.Tx, sb *sandbox, drop func(string) bool, add func(*tar.Writer, map[string]byte) error) (string, error) {
	var old io.Reader
	if sb.snapshotKey != nil {
		rc, err := s.store.Get(ctx, *sb.snapshotKey)
		if err != nil {
			return "", err
		}
		defer rc.Close()
		old = rc
	}
	key := s.store.NewKey(sb.SandboxID)
	pr, pw := io.Pipe()
	rewriteErr := make(chan error, 1)
	go func() {
		err := workspace.Rewrite(old, pw, drop, add)
		pw.CloseWithError(err)
		rewriteErr <- err
	}()
	size, err := s.store.Put(ctx, key, pr)
	pr.CloseWithError(io.ErrClosedPipe)
	if rerr := <-rewriteErr; rerr != nil {
		return "", rerr // the edit failed (conflict, bad upload...): report that
	}
	if err != nil {
		return "", err
	}
	return recordSnapshot(ctx, tx, sb, key, size)
}

// commitRewrite commits a paused write and drops the replaced snapshot.
func (s *server) commitRewrite(w http.ResponseWriter, r *http.Request, tx pgx.Tx, oldKey string) bool {
	if err := tx.Commit(r.Context()); err != nil {
		s.internalError(w, err)
		return false
	}
	s.dropSnapshot(r.Context(), oldKey)
	return true
}

// runSnapshots runs the restore and expiry loops until ctx is done.
func (s *server) runSnapshots(ctx context.Context) {
	restoreTick := time.NewTicker(2 * time.Second)
	expiryTick := time.NewTicker(10 * time.Second)
	defer restoreTick.Stop()
	defer expiryTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-restoreTick.C:
			s.restorePending(ctx)
		case <-s.restoreKick:
			s.restorePending(ctx)
		case <-expiryTick.C:
			s.snapshotExpiring(ctx)
		}
	}
}

// kickRestores makes the restore loop look for waiting pods now (e.g.
// right after a resume) instead of on its next tick.
func (s *server) kickRestores() {
	select {
	case s.restoreKick <- struct{}{}:
	default:
	}
}

// restores tracks restores in progress on this replica, by pod UID, and
// recently finished ones (a restored pod's wait container takes a moment to
// exit, and must not be restored twice).
var restores = struct {
	sync.Mutex
	active map[string]time.Time
}{active: map[string]time.Time{}}

// restorePending finds sandbox pods waiting for the agent and, unless
// they're waiting for staged files, has the agent restore their snapshot
// (or just release them if there's none). Every replica runs this; a
// per-pod advisory lock makes sure only one restores a given pod.
func (s *server) restorePending(ctx context.Context) {
	pods, err := s.core.CoreV1().Pods(s.sandboxNamespace).List(ctx, metav1.ListOptions{LabelSelector: "webapp/sandbox-id"})
	if err != nil {
		log.Printf("restore: list pods: %v", err)
		return
	}
	restores.Lock()
	defer restores.Unlock()
	for uid, at := range restores.active {
		if time.Since(at) > time.Minute {
			delete(restores.active, uid)
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		uid := string(pod.UID)
		if pod.DeletionTimestamp != nil || !waitingForAgent(pod) {
			continue
		}
		if _, busy := restores.active[uid]; busy {
			continue
		}
		restores.active[uid] = time.Now()
		go s.restorePod(context.WithoutCancel(ctx), pod)
	}
}

func (s *server) restorePod(ctx context.Context, pod *corev1.Pod) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	id := pod.Labels["webapp/sandbox-id"]
	tx, err := s.db.Begin(ctx)
	if err != nil {
		log.Printf("restore: %v", err)
		return
	}
	defer tx.Rollback(ctx)
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('restore:' || $1))`, string(pod.UID)).Scan(&locked); err != nil || !locked {
		return // another replica is on it
	}
	sb, err := scanSandbox(tx.QueryRow(ctx, `SELECT `+sandboxColumns+` FROM sandboxes WHERE sandbox_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return
	}
	if err != nil {
		log.Printf("restore %s: %v", id, err)
		return
	}
	if sb.stageFiles && sb.stagedAt == nil {
		return // the client releases it once its files are staged
	}
	key := ""
	if sb.snapshotKey != nil {
		key = *sb.snapshotKey
	}
	start := time.Now()
	var out struct{ Files, Bytes int64 }
	if err := s.agent.call(ctx, pod, http.MethodPost, "/restore", map[string]string{"key": key}, &out); err != nil {
		log.Printf("restore sandbox %s into pod %s: %v", id, pod.Name, err)
		restores.Lock()
		delete(restores.active, string(pod.UID)) // retry on the next pass
		restores.Unlock()
		return
	}
	log.Printf("restored sandbox %s: %d files, %d bytes in %s", id, out.Files, out.Bytes, time.Since(start).Round(time.Millisecond))
}

// snapshotExpiring saves the workspace of running sandboxes that expire
// within expirySnapshotLead and haven't been saved in that window yet, so
// their files outlive them for the recovery window. Rows are locked with
// SKIP LOCKED, so replicas share the work.
func (s *server) snapshotExpiring(ctx context.Context) {
	rows, err := s.db.Query(ctx, `SELECT sandbox_id::text FROM sandboxes
		WHERE deleted_at IS NULL AND volume_name IS NULL AND volume_size IS NOT NULL
		  AND operating_mode = $1 AND expires_at > now() AND expires_at < $2
		  AND (snapshot_at IS NULL OR snapshot_at < expires_at - make_interval(secs => $3))`,
		modeRunning, time.Now().Add(expirySnapshotLead), expirySnapshotLead.Seconds())
	if err != nil {
		log.Printf("expiry snapshots: %v", err)
		return
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		log.Printf("expiry snapshots: %v", err)
		return
	}
	for _, id := range ids {
		s.snapshotOne(ctx, id)
	}
}

func (s *server) snapshotOne(ctx context.Context, id string) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)
	sb, err := scanSandbox(tx.QueryRow(ctx, `SELECT `+sandboxColumns+` FROM sandboxes
		WHERE sandbox_id = $1 AND operating_mode = $2 AND deleted_at IS NULL FOR UPDATE SKIP LOCKED`, id, modeRunning))
	if err != nil {
		return // gone, paused, or another replica has it
	}
	key, size, took, err := s.snapshotLive(ctx, sb)
	if err != nil {
		log.Printf("expiry snapshot of sandbox %s: %v", id, err)
		return
	}
	if !took {
		// Nothing to save; still mark the window as done.
		tx.Exec(ctx, `UPDATE sandboxes SET snapshot_at = now() WHERE sandbox_id = $1 AND snapshot_key IS NULL`, id)
		tx.Commit(ctx)
		return
	}
	old, err := recordSnapshot(ctx, tx, &sb, key, size)
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		log.Printf("expiry snapshot of sandbox %s: %v", id, err)
		s.dropSnapshot(ctx, key)
		return
	}
	s.dropSnapshot(ctx, old)
}

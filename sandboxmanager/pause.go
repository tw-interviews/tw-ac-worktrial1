package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// Pausing and resuming use the upstream Sandbox's spec.operatingMode
// (https://agent-sandbox.sigs.k8s.io/docs/sandbox/conditions/). Pausing first
// saves /workspace to S3 (a snapshot, see snapshots.go), then suspends, which
// deletes only the pod. While paused, files are read from (and written to)
// the snapshot. Memory, processes and the rest of the container filesystem
// don't survive. Resuming starts a new pod, whose wait init container holds
// it until the snapshot has been restored into its workspace.
//
// Both endpoints return 202 once the change is made; clients poll GET for
// Paused or Ready. Both are idempotent, and resuming while still Pausing is
// fine (upstream supports flipping back mid-suspension).

var errSandboxPaused = errors.New("sandbox is paused")

const (
	codeSandboxPaused = "sandbox_paused"
	msgSandboxPaused  = "sandbox is paused; resume it with POST /sandboxes/{id}/resume"
	msgSandboxGone    = "sandbox no longer exists in the cluster (expired or deleted)"
)

// runningExecutionWindow bounds which "running" executions block a pause:
// none can run longer than maxExecTimeout, so older running rows are stale
// (e.g. left by a server restart).
const runningExecutionWindow = maxExecTimeout + time.Minute

// pauseSandbox suspends a sandbox: POST /sandboxes/{id}/pause. It refuses
// (409) while commands are running, since stopping the pod kills them, unless
// ?force=true. The sandbox then expires after its paused TTL.
func (s *server) pauseSandbox(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tx, sb, ok := s.lockSandbox(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(ctx)

	switch {
	case sb.gone():
		writeError(w, http.StatusConflict, msgSandboxGone)
		return
	case !sb.usesSnapshots() || sb.PausedTTLSeconds == nil:
		writeError(w, http.StatusConflict, "sandbox was created before pausing was supported and can't be paused")
		return
	case sb.stageFiles && sb.stagedAt == nil:
		writeError(w, http.StatusConflict, "sandbox is still staging files; complete staging first")
		return
	case sb.operatingMode == modeSuspended:
		s.writeSandbox(w, ctx, sb, http.StatusAccepted)
		return
	}

	// Executions take the same row lock before they're recorded (see
	// createExecution), so none can start between this check and the pause.
	if r.URL.Query().Get("force") != "true" {
		var running int
		err := tx.QueryRow(ctx,
			`SELECT count(*) FROM executions WHERE sandbox_id = $1 AND status = $2 AND started_at > $3`,
			sb.SandboxID, execRunning, time.Now().Add(-runningExecutionWindow),
		).Scan(&running)
		if err != nil {
			s.internalError(w, err)
			return
		}
		if running > 0 {
			writeError(w, http.StatusConflict, fmt.Sprintf(
				"%d command(s) still running in the sandbox; wait for them to finish, or pause with ?force=true to stop them", running))
			return
		}
	}

	// Save the workspace before the pod goes. The row lock keeps executions
	// and file writes out until the pause is committed.
	obj, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Get(ctx, sb.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusConflict, msgSandboxGone)
		return
	}
	if err != nil {
		log.Printf("get sandbox %s: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to read sandbox from cluster")
		return
	}
	key, size, took, err := s.snapshotLive(ctx, sb)
	if err != nil {
		log.Printf("snapshot sandbox %s for pause: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to save the sandbox's workspace; nothing was changed")
		return
	}
	oldKey := ""
	if took {
		if oldKey, err = recordSnapshot(ctx, tx, &sb, key, size); err != nil {
			s.internalError(w, err)
			return
		}
	}
	// From now on every new pod of this sandbox waits for its restore.
	spec, err := podTemplateSpec(obj)
	if err != nil {
		s.internalError(w, err)
		return
	}
	gated := withGate(spec)

	expiresAt := expiryAfter(time.Duration(*sb.PausedTTLSeconds) * time.Second)
	err = tx.QueryRow(ctx,
		`UPDATE sandboxes SET operating_mode = $2, paused_at = now(), expires_at = $3 WHERE sandbox_id = $1 RETURNING paused_at`,
		sb.SandboxID, modeSuspended, expiresAt,
	).Scan(&sb.PausedAt)
	if err != nil {
		s.internalError(w, err)
		return
	}
	sb.operatingMode, sb.ExpiresAt = modeSuspended, &expiresAt
	if s.commitMode(w, r, tx, sb, &gated) {
		s.dropSnapshot(ctx, oldKey)
	} else if took {
		s.dropSnapshot(ctx, key)
	}
}

// resumeSandbox starts a paused sandbox again: POST /sandboxes/{id}/resume.
// Its new pod is checked against the quota first (429 if it wouldn't fit),
// and its TTL restarts from now.
func (s *server) resumeSandbox(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tx, sb, ok := s.lockSandbox(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(ctx)

	if sb.gone() {
		msg := msgSandboxGone
		if until := sb.retainedUntil(s.volumeRetention); until != nil {
			msg += fmt.Sprintf("; its files can still be downloaded until %s", until.UTC().Format(time.RFC3339))
		}
		writeError(w, http.StatusConflict, msg)
		return
	}
	if sb.operatingMode != modeSuspended {
		s.writeSandbox(w, ctx, sb, http.StatusAccepted)
		return
	}

	obj, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Get(ctx, sb.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusConflict, msgSandboxGone)
		return
	}
	if err != nil {
		log.Printf("get sandbox %s: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to read sandbox from cluster")
		return
	}
	// As at creation: refuse now rather than leave the sandbox Resuming with
	// its pod rejected by the quota.
	spec, err := podTemplateSpec(obj)
	if err != nil {
		s.internalError(w, err)
		return
	}
	if status, msg := s.admitSandboxPod(ctx, sb, spec); status != 0 {
		writeError(w, status, msg)
		return
	}
	expiresAt := expiryAfter(time.Duration(*sb.TTLSeconds) * time.Second)
	err = tx.QueryRow(ctx,
		`UPDATE sandboxes SET operating_mode = $2, paused_at = NULL, resumed_at = now(), expires_at = $3
		WHERE sandbox_id = $1 RETURNING resumed_at`,
		sb.SandboxID, modeRunning, expiresAt,
	).Scan(&sb.resumedAt)
	if err != nil {
		s.internalError(w, err)
		return
	}
	sb.operatingMode, sb.ExpiresAt, sb.PausedAt = modeRunning, &expiresAt, nil
	if s.commitMode(w, r, tx, sb, nil) {
		s.kickRestores() // the new pod waits for its snapshot
	}
}

// lockSandbox loads the {id} sandbox in a new transaction, holding its row
// lock, or writes an error response and returns ok=false. The caller rolls
// tx back (a no-op after commit).
func (s *server) lockSandbox(w http.ResponseWriter, r *http.Request) (tx pgx.Tx, sb sandbox, ok bool) {
	sb, ok = s.loadSandbox(w, r, r.PathValue("id"))
	if !ok {
		return nil, sb, false
	}
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		s.internalError(w, err)
		return nil, sb, false
	}
	sb, err = scanSandbox(tx.QueryRow(r.Context(),
		`SELECT `+sandboxColumns+` FROM sandboxes WHERE sandbox_id = $1 FOR UPDATE`, sb.SandboxID))
	if err != nil {
		tx.Rollback(r.Context())
		s.internalError(w, err) // deleted in between; rare enough not to special-case
		return nil, sb, false
	}
	return tx, sb, true
}

// commitMode patches the Sandbox to sb's operating mode and expiry (and,
// if podSpec isn't nil, its pod template's spec), commits tx, and responds
// 202 with the sandbox's new status. Returns whether it all went through.
func (s *server) commitMode(w http.ResponseWriter, r *http.Request, tx pgx.Tx, sb sandbox, podSpec *corev1.PodSpec) bool {
	ctx := r.Context()
	spec := map[string]any{"operatingMode": sb.operatingMode, "shutdownTime": sb.ExpiresAt.UTC().Format(time.RFC3339)}
	if podSpec != nil {
		spec["podTemplate"] = map[string]any{"spec": podSpec}
	}
	patch, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		s.internalError(w, err)
		return false
	}
	obj, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Patch(ctx, sb.Name,
		types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusConflict, msgSandboxGone)
		return false
	}
	if err != nil {
		log.Printf("set operating mode of sandbox %s: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to update sandbox in cluster")
		return false
	}
	if err := tx.Commit(ctx); err != nil {
		// The cluster already has the new mode; the next pause/resume call
		// reconciles the record with it.
		s.internalError(w, err)
		return false
	}
	sb.observe(obj, s.volumeRetention)
	writeJSON(w, http.StatusAccepted, sb)
	return true
}

// podTemplateSpec returns the pod spec the Sandbox's pods are created from.
func podTemplateSpec(obj *unstructured.Unstructured) (corev1.PodSpec, error) {
	raw, found, err := unstructured.NestedMap(obj.Object, "spec", "podTemplate", "spec")
	if err != nil || !found {
		return corev1.PodSpec{}, fmt.Errorf("sandbox %s has no pod template", obj.GetName())
	}
	var spec corev1.PodSpec
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &spec)
	return spec, err
}

// specMode is the Sandbox's desired operating mode.
func specMode(obj *unstructured.Unstructured) string {
	if mode, _, _ := unstructured.NestedString(obj.Object, "spec", "operatingMode"); mode != "" {
		return mode
	}
	return modeRunning
}

// suspended reports whether the controller has finished suspending the
// Sandbox: Suspended=True, observed for its current spec.
func suspended(obj *unstructured.Unstructured) bool {
	c := findCondition(obj, "Suspended")
	if c == nil || c["status"] != "True" {
		return false
	}
	gen, _ := c["observedGeneration"].(int64)
	return gen == obj.GetGeneration()
}

// conditionChangedSince reports whether the condition's status changed at or
// after t (lastTransitionTime has second precision).
func conditionChangedSince(obj *unstructured.Unstructured, typ string, t time.Time) bool {
	c := findCondition(obj, typ)
	if c == nil {
		return false
	}
	raw, _ := c["lastTransitionTime"].(string)
	changed, err := time.Parse(time.RFC3339, raw)
	return err == nil && !changed.Before(t.Truncate(time.Second))
}

func findCondition(obj *unstructured.Unstructured, typ string) map[string]any {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conditions {
		if cond, ok := c.(map[string]any); ok && cond["type"] == typ {
			return cond
		}
	}
	return nil
}

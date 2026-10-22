package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

// sandboxGVR is the upstream kubernetes-sigs agent-sandbox Sandbox resource.
var sandboxGVR = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}

// userContainerName is the one container in a sandbox pod that runs the
// user's image and receives executions. Any other containers in the pod are
// control-plane sidecars and are never targeted by user requests.
const userContainerName = "sandbox"

// Sandbox TTLs. A sandbox is deleted by the agent-sandbox controller once its
// spec.shutdownTime passes (shutdownPolicy: Delete); every execution pushes
// shutdownTime out again (see extendTTL).
const (
	defaultSandboxTTL = 5 * time.Minute
	minSandboxTTL     = 30 * time.Second
	maxSandboxTTL     = 24 * time.Hour
)

// A paused sandbox has its own, longer TTL, because the Sandbox's
// shutdownTime still applies while it's paused: when it passes, the Sandbox is
// deleted and can't be resumed (its snapshot stays for the recovery window).
const (
	defaultPausedTTL = 24 * time.Hour
	minPausedTTL     = time.Minute
	maxPausedTTL     = 7 * 24 * time.Hour
)

// Values of the Sandbox's spec.operatingMode, mirrored in sandboxes.operating_mode.
const (
	modeRunning   = "Running"
	modeSuspended = "Suspended"
)

// sandboxGracePeriod is how long a deleted sandbox's container gets to exit.
// Kept short because typical sandbox commands (e.g. `sleep infinity` as PID 1)
// ignore SIGTERM, and a terminating pod still counts against the quota.
var sandboxGracePeriod int64 = 5

var noAutomount, noServiceLinks = false, false

// Sandbox statuses returned by the API.
const (
	statusStaging  = "Staging" // waiting for files to be uploaded before the user container starts
	statusReady    = "Ready"
	statusNotReady = "NotReady"
	statusPausing  = "Pausing"  // pause requested; the pod is stopping
	statusPaused   = "Paused"   // pod stopped; /workspace saved to a snapshot, its files still accessible
	statusResuming = "Resuming" // resume requested; a new pod is starting and restoring the snapshot
	statusDeleting = "Deleting"
	statusDeleted  = "Deleted" // deleted via the API; files recoverable until files_retained_until
	statusExpired  = "Expired" // TTL passed and the controller deleted it
	statusMissing  = "Missing" // gone from the cluster for another reason
)

type sandboxRequest struct {
	Image      string            `json:"image"`
	Command    []string          `json:"command,omitempty"`
	Args       []string          `json:"args,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	TTLSeconds int               `json:"ttl_seconds,omitempty"`
	// PausedTTLSeconds is how long the sandbox may stay paused before it
	// expires. Default 24h.
	PausedTTLSeconds int `json:"paused_ttl_seconds,omitempty"`
	// StageFiles holds the user container back until POST
	// /sandboxes/{id}/staging/complete, so files uploaded in between are in
	// /workspace before its command starts.
	StageFiles bool `json:"stage_files,omitempty"`
	Resources  struct {
		Requests map[string]string `json:"requests,omitempty"`
		Limits   map[string]string `json:"limits,omitempty"`
	} `json:"resources"`
}

type sandbox struct {
	SandboxID  string    `json:"sandbox_id"`
	Name       string    `json:"name"`
	Namespace  string    `json:"namespace"`
	Definition string    `json:"definition"`
	CreatedAt  time.Time `json:"created_at"`
	// TTLSeconds and ExpiresAt are null for sandboxes created before TTLs
	// existed; those never expire.
	TTLSeconds *int       `json:"ttl_seconds"`
	ExpiresAt  *time.Time `json:"expires_at"`
	// Volume is the PVC that held /workspace for sandboxes created when
	// sandboxes had persistent volumes; null since workspaces are emptyDirs
	// saved as snapshots. VolumeSize is the workspace's size limit (null for
	// sandboxes from before workspaces existed).
	Volume     *string    `json:"volume"`
	VolumeSize *string    `json:"volume_size"`
	DeletedAt  *time.Time `json:"deleted_at"`
	// SnapshotAt and SnapshotBytes describe the latest saved copy of
	// /workspace (see snapshots.go); null until the first one.
	SnapshotAt    *time.Time `json:"snapshot_at"`
	SnapshotBytes *int64     `json:"snapshot_bytes"`
	// FilesRetainedUntil is set once the sandbox is gone (deleted or expired)
	// and a snapshot of its files exists: they can still be downloaded until
	// then, after which the snapshot is deleted. See gc.go.
	FilesRetainedUntil *time.Time `json:"files_retained_until"`
	// PausedTTLSeconds is the TTL while paused (null for sandboxes created
	// before pausing existed: they can't be paused). PausedAt is set while
	// the sandbox is paused.
	PausedTTLSeconds *int       `json:"paused_ttl_seconds"`
	PausedAt         *time.Time `json:"paused_at"`
	// Status is read live from the cluster on GET; see the status* constants.
	Status string `json:"status,omitempty"`

	stageFiles    bool
	stagedAt      *time.Time
	operatingMode string     // the mode last set through the API
	resumedAt     *time.Time // when it was last resumed, to tell Resuming from NotReady
	snapshotKey   *string    // S3 key of the latest snapshot
}

const sandboxColumns = `sandbox_id::text, name, namespace, definition, created_at, ttl_seconds, expires_at,
	volume_name, volume_size, deleted_at, stage_files, staged_at,
	paused_ttl_seconds, paused_at, operating_mode, resumed_at,
	snapshot_key, snapshot_bytes, snapshot_at`

type kubeClients struct {
	dynamic   dynamic.Interface
	core      kubernetes.Interface
	config    *rest.Config
	namespace string
}

// newKubeClients uses the in-cluster service account when running in a pod,
// and falls back to ~/.kube/config (or $KUBECONFIG) when run locally. The
// target namespace comes from SANDBOX_NAMESPACE, else the pod's own namespace
// or the kubeconfig context's namespace.
func newKubeClients() (*kubeClients, error) {
	cfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	restCfg, err := cfg.ClientConfig()
	if err != nil {
		return nil, err
	}
	// Throughput tuning, for the benchmark instance; unset keeps client-go's
	// defaults (5 QPS, burst 10, one multiplexed HTTP/2 connection).
	// KUBE_QPS < 0 turns off client-side rate limiting. KUBE_HTTP1=true uses a
	// connection per in-flight request: one HTTP/2 connection carries at most
	// ~100 concurrent requests (the API server's stream limit).
	if v := os.Getenv("KUBE_QPS"); v != "" {
		qps, err := strconv.ParseFloat(v, 32)
		if err != nil {
			return nil, fmt.Errorf("KUBE_QPS: %w", err)
		}
		restCfg.QPS = float32(qps)
	}
	if v := os.Getenv("KUBE_BURST"); v != "" {
		if restCfg.Burst, err = strconv.Atoi(v); err != nil {
			return nil, fmt.Errorf("KUBE_BURST: %w", err)
		}
	}
	if os.Getenv("KUBE_HTTP1") == "true" {
		restCfg.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	ns := os.Getenv("SANDBOX_NAMESPACE")
	if ns == "" {
		if ns, _, err = cfg.Namespace(); err != nil {
			return nil, err
		}
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}
	core, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}
	return &kubeClients{dynamic: dyn, core: core, config: restCfg, namespace: ns}, nil
}

func (s *server) createSandbox(w http.ResponseWriter, r *http.Request) {
	var req sandboxRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	userSpec, err := req.podSpec()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ttl := defaultSandboxTTL
	if req.TTLSeconds != 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
		if ttl < minSandboxTTL || ttl > maxSandboxTTL {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("ttl_seconds must be between %d and %d",
				int(minSandboxTTL.Seconds()), int(maxSandboxTTL.Seconds())))
			return
		}
	}
	ttlSeconds := int(ttl.Seconds())
	expiresAt := expiryAfter(ttl)
	pausedTTL := defaultPausedTTL
	if req.PausedTTLSeconds != 0 {
		pausedTTL = time.Duration(req.PausedTTLSeconds) * time.Second
		if pausedTTL < minPausedTTL || pausedTTL > maxPausedTTL {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("paused_ttl_seconds must be between %d and %d",
				int(minPausedTTL.Seconds()), int(maxPausedTTL.Seconds())))
			return
		}
	}
	pausedTTLSeconds := int(pausedTTL.Seconds())

	volumeSize := defaultVolumeSize
	sb := sandbox{
		SandboxID:  uuid.NewString(),
		Namespace:  s.sandboxNamespace,
		TTLSeconds: &ttlSeconds,
		ExpiresAt:  &expiresAt,
		VolumeSize: &volumeSize,
		stageFiles: req.StageFiles,

		PausedTTLSeconds: &pausedTTLSeconds,
		operatingMode:    modeRunning,
	}
	sb.Name = "sbx-" + sb.SandboxID
	podSpec := withWorkspace(userSpec, req.StageFiles)

	// Check the pod against the namespace's ResourceQuota and LimitRange now.
	// Otherwise the Sandbox would be created but its pod rejected, leaving it
	// NotReady until it expires.
	ctx := r.Context()
	if status, msg := s.admitSandboxPod(ctx, sb, podSpec); status != 0 {
		writeError(w, status, msg)
		return
	}

	obj := map[string]any{
		"apiVersion": sandboxGVR.GroupVersion().String(),
		"kind":       "Sandbox",
		"metadata": map[string]any{
			"name":      sb.Name,
			"namespace": sb.Namespace,
			"labels":    map[string]any{"webapp/sandbox-id": sb.SandboxID},
		},
		"spec": map[string]any{
			"shutdownTime":   expiresAt.Format(time.RFC3339),
			"shutdownPolicy": "Delete",
			"podTemplate": map[string]any{
				"metadata": map[string]any{
					// The restore loop finds waiting pods by this label (snapshots.go).
					"labels": map[string]any{"webapp/sandbox-id": sb.SandboxID},
					// Lets kubectl exec/logs pick the user container.
					"annotations": map[string]any{"kubectl.kubernetes.io/default-container": userContainerName},
				},
				"spec": podSpec,
			},
		},
	}
	def, err := yaml.Marshal(obj)
	if err != nil {
		s.internalError(w, err)
		return
	}
	sb.Definition = string(def)

	// Record the sandbox in a transaction that commits only once the
	// Kubernetes object exists, so a failed create leaves no orphan row.
	tx, err := s.db.Begin(ctx)
	if err != nil {
		s.internalError(w, err)
		return
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx,
		`INSERT INTO sandboxes (sandbox_id, name, namespace, definition, ttl_seconds, expires_at,
			volume_name, volume_size, stage_files, paused_ttl_seconds, operating_mode)
		VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10) RETURNING created_at`,
		sb.SandboxID, sb.Name, sb.Namespace, sb.Definition, ttlSeconds, expiresAt,
		volumeSize, sb.stageFiles, pausedTTLSeconds, modeRunning,
	).Scan(&sb.CreatedAt)
	if err != nil {
		s.internalError(w, err)
		return
	}

	var u unstructured.Unstructured
	if err := yaml.Unmarshal(def, &u.Object); err != nil {
		s.internalError(w, err)
		return
	}
	if _, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Create(ctx, &u, metav1.CreateOptions{}); err != nil {
		if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		log.Printf("create sandbox %s: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to create sandbox in cluster")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		// Don't leave a cluster object without a database record.
		if delErr := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Delete(ctx, sb.Name, metav1.DeleteOptions{}); delErr != nil {
			log.Printf("cleanup sandbox %s: %v", sb.Name, delErr)
		}
		s.internalError(w, err)
		return
	}
	if sb.stageFiles {
		sb.Status = statusStaging
	}
	writeJSON(w, http.StatusCreated, sb)
}

func (s *server) listSandboxes(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `SELECT `+sandboxColumns+` FROM sandboxes ORDER BY created_at`)
	if err != nil {
		s.internalError(w, err)
		return
	}
	sandboxes, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sandbox, error) {
		return scanSandbox(row)
	})
	if err != nil {
		s.internalError(w, err)
		return
	}

	// One List call per namespace for every Sandbox this service created,
	// rather than a Get per row. Older sandboxes may live outside
	// s.sandboxNamespace, so list each namespace the rows mention.
	live := map[string]*unstructured.Unstructured{}
	listed := map[string]bool{}
	for _, sb := range sandboxes {
		if listed[sb.Namespace] {
			continue
		}
		listed[sb.Namespace] = true
		list, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).List(r.Context(),
			metav1.ListOptions{LabelSelector: "webapp/sandbox-id"})
		if err != nil {
			log.Printf("list sandboxes in %s: %v", sb.Namespace, err)
			writeError(w, http.StatusBadGateway, "failed to read sandboxes from cluster")
			return
		}
		for i := range list.Items {
			live[list.Items[i].GetNamespace()+"/"+list.Items[i].GetName()] = &list.Items[i]
		}
	}

	for i := range sandboxes {
		sandboxes[i].observe(live[sandboxes[i].Namespace+"/"+sandboxes[i].Name], s.volumeRetention)
	}
	writeJSON(w, http.StatusOK, sandboxes)
}

func (s *server) getSandbox(w http.ResponseWriter, r *http.Request) {
	sb, ok := s.loadSandbox(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	s.writeSandbox(w, r.Context(), sb, http.StatusOK)
}

// writeSandbox responds with sb and its live status.
func (s *server) writeSandbox(w http.ResponseWriter, ctx context.Context, sb sandbox, status int) {
	obj, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Get(ctx, sb.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Printf("get sandbox %s: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to read sandbox from cluster")
		return
	}
	if err != nil {
		obj = nil
	}
	sb.observe(obj, s.volumeRetention)
	writeJSON(w, status, sb)
}

// deleteSandbox removes the Sandbox from the cluster (which garbage-collects
// its pod). It doesn't wait for the pod to stop: the cluster tears it down in
// the background within sandboxGracePeriod.
//
// A running sandbox's workspace is saved first (a snapshot, see
// snapshots.go); it and the database record are kept for the recovery
// window (s.volumeRetention), during which files can still be downloaded;
// gc.go deletes them afterwards. ?purge=true deletes everything immediately,
// as does deleting a sandbox from before workspaces existed. If anything
// fails nothing changes, so the caller can retry.
func (s *server) deleteSandbox(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tx, sb, ok := s.lockSandbox(w, r)
	if !ok {
		return
	}
	defer tx.Rollback(ctx)
	keep := r.URL.Query().Get("purge") != "true" && sb.VolumeSize != nil

	oldKey, newKey := "", ""
	if keep && sb.usesSnapshots() && !sb.gone() && sb.operatingMode == modeRunning {
		key, size, took, err := s.snapshotLive(ctx, sb)
		if err != nil {
			log.Printf("snapshot sandbox %s for delete: %v", sb.Name, err)
			writeError(w, http.StatusBadGateway, "failed to save the sandbox's workspace; retry, or delete with ?purge=true")
			return
		}
		if took {
			newKey = key
			if oldKey, err = recordSnapshot(ctx, tx, &sb, key, size); err != nil {
				s.internalError(w, err)
				return
			}
		}
	}

	err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Delete(ctx, sb.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Printf("delete sandbox %s: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to delete sandbox from cluster")
		s.dropSnapshot(ctx, newKey)
		return
	}

	if keep {
		_, err = tx.Exec(ctx, `UPDATE sandboxes SET deleted_at = COALESCE(deleted_at, now()) WHERE sandbox_id = $1`, sb.SandboxID)
	} else {
		if err := s.deleteFiles(ctx, sb); err != nil {
			log.Printf("delete files of sandbox %s: %v", sb.Name, err)
			writeError(w, http.StatusBadGateway, "failed to delete sandbox files")
			return
		}
		_, err = tx.Exec(ctx, `DELETE FROM sandboxes WHERE sandbox_id = $1`, sb.SandboxID)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		s.internalError(w, err)
		return
	}
	s.dropSnapshot(ctx, oldKey)
	w.WriteHeader(http.StatusNoContent)
}

// deleteFiles deletes everything kept for a sandbox's files: its snapshots,
// or a legacy sandbox's volume.
func (s *server) deleteFiles(ctx context.Context, sb sandbox) error {
	if sb.usesSnapshots() {
		return s.store.DeleteAll(ctx, sb.SandboxID)
	}
	return s.deleteLegacyVolume(ctx, sb)
}

// scanSandbox reads one row selected with sandboxColumns.
func scanSandbox(row pgx.Row) (sandbox, error) {
	var sb sandbox
	err := row.Scan(&sb.SandboxID, &sb.Name, &sb.Namespace, &sb.Definition, &sb.CreatedAt, &sb.TTLSeconds, &sb.ExpiresAt,
		&sb.Volume, &sb.VolumeSize, &sb.DeletedAt, &sb.stageFiles, &sb.stagedAt,
		&sb.PausedTTLSeconds, &sb.PausedAt, &sb.operatingMode, &sb.resumedAt,
		&sb.snapshotKey, &sb.SnapshotBytes, &sb.SnapshotAt)
	return sb, err
}

// gone reports whether the sandbox has been deleted through the API or has
// certainly expired (its controller deletes it at expires_at).
func (sb sandbox) gone() bool {
	return sb.DeletedAt != nil || (sb.ExpiresAt != nil && !time.Now().Before(*sb.ExpiresAt))
}

// usesSnapshots reports whether the sandbox's workspace is an emptyDir saved
// as snapshots, rather than a legacy persistent volume (or, for the oldest
// sandboxes, no workspace at all).
func (sb sandbox) usesSnapshots() bool { return sb.Volume == nil && sb.VolumeSize != nil }

// retainedUntil is when a gone sandbox's kept files (its snapshot, or a
// legacy volume) will be deleted; nil if nothing was kept.
func (sb sandbox) retainedUntil(retention time.Duration) *time.Time {
	var since *time.Time
	switch {
	case sb.Volume == nil && sb.snapshotKey == nil:
		return nil
	case sb.DeletedAt != nil:
		since = sb.DeletedAt
	case sb.ExpiresAt != nil && !time.Now().Before(*sb.ExpiresAt):
		since = sb.ExpiresAt
	default:
		return nil
	}
	t := since.Add(retention)
	return &t
}

// extendTTL pushes a running sandbox's expiry out to at least now + ttl +
// extra; see extendTTLTx.
func (s *server) extendTTL(ctx context.Context, sb sandbox, extra time.Duration) (*time.Time, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	expiresAt, err := s.extendTTLTx(ctx, tx, sb, extra)
	if err != nil {
		return nil, err
	}
	return expiresAt, tx.Commit(ctx)
}

// extendTTLTx pushes the sandbox's expiry out to at least now + ttl + extra,
// both in the database and in the Sandbox's spec.shutdownTime, as part of tx.
// It never shortens the expiry, and the row lock taken by the UPDATE
// serializes concurrent callers (and pauseSandbox) so the cluster always ends
// up with the latest value. Returns errSandboxPaused if the sandbox is
// paused, and leaves sandboxes without a TTL alone. Returns the new expiry.
func (s *server) extendTTLTx(ctx context.Context, tx pgx.Tx, sb sandbox, extra time.Duration) (*time.Time, error) {
	if sb.TTLSeconds == nil {
		return nil, nil
	}
	target := expiryAfter(time.Duration(*sb.TTLSeconds)*time.Second + extra)
	var expiresAt time.Time
	err := tx.QueryRow(ctx,
		`UPDATE sandboxes SET expires_at = GREATEST(expires_at, $2)
		WHERE sandbox_id = $1 AND operating_mode = $3 RETURNING expires_at`,
		sb.SandboxID, target, modeRunning,
	).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errSandboxPaused
	}
	if err != nil {
		return nil, err
	}
	patch := fmt.Sprintf(`{"spec":{"shutdownTime":%q}}`, expiresAt.UTC().Format(time.RFC3339))
	if _, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Patch(ctx, sb.Name,
		types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return nil, err
	}
	return &expiresAt, nil
}

// expiryAfter returns now + d, rounded up to a whole second because
// spec.shutdownTime only has second precision.
func expiryAfter(d time.Duration) time.Time {
	return time.Now().UTC().Add(d).Truncate(time.Second).Add(time.Second)
}

// admitSandboxPod dry-runs creating the sandbox's pod so the namespace's
// LimitRange and ResourceQuota are applied before anything is created. It
// returns 0 if the pod would be admitted, else the HTTP status and message.
func (s *server) admitSandboxPod(ctx context.Context, sb sandbox, spec corev1.PodSpec) (int, string) {
	// A generated name, so a resume can be checked while the old pod (with
	// the sandbox's name) is still terminating.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: sb.Name + "-", Namespace: sb.Namespace},
		Spec:       spec,
	}
	_, err := s.core.CoreV1().Pods(sb.Namespace).Create(ctx, pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	switch {
	case err == nil:
		return 0, ""
	case apierrors.IsForbidden(err) && strings.Contains(err.Error(), "exceeded quota"):
		return http.StatusTooManyRequests, "sandbox quota exceeded; delete sandboxes or request fewer resources: " + err.Error()
	case apierrors.IsForbidden(err) || apierrors.IsInvalid(err):
		return http.StatusBadRequest, err.Error()
	default:
		log.Printf("dry-run sandbox pod %s: %v", sb.Name, err)
		return http.StatusBadGateway, "failed to validate sandbox with cluster"
	}
}

// observe sets Status from the live Sandbox object (nil if it's gone), and
// FilesRetainedUntil for sandboxes that are gone.
func (sb *sandbox) observe(obj *unstructured.Unstructured, retention time.Duration) {
	switch {
	case sb.DeletedAt != nil:
		sb.Status = statusDeleted
	case obj == nil && sb.ExpiresAt != nil && !time.Now().Before(*sb.ExpiresAt):
		sb.Status = statusExpired
	case obj == nil:
		sb.Status = statusMissing
	case obj.GetDeletionTimestamp() != nil:
		sb.Status = statusDeleting
	case specMode(obj) == modeSuspended && suspended(obj):
		sb.Status = statusPaused
	case specMode(obj) == modeSuspended:
		sb.Status = statusPausing
	default:
		sb.Status = sandboxStatus(obj)
		switch {
		case sb.Status != statusNotReady:
		case sb.stageFiles && sb.stagedAt == nil:
			sb.Status = statusStaging
		case sb.resumedAt != nil && !conditionChangedSince(obj, "Ready", *sb.resumedAt):
			sb.Status = statusResuming
		}
	}
	sb.FilesRetainedUntil = sb.retainedUntil(retention)
}

// loadSandbox looks up the sandbox with the given id, writing a 400 or 404
// response and returning false if it can't.
func (s *server) loadSandbox(w http.ResponseWriter, r *http.Request, rawID string) (sandbox, bool) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "sandbox id must be a UUID")
		return sandbox{}, false
	}
	sb, err := scanSandbox(s.db.QueryRow(r.Context(), `SELECT `+sandboxColumns+` FROM sandboxes WHERE sandbox_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "sandbox not found")
		return sandbox{}, false
	}
	if err != nil {
		s.internalError(w, err)
		return sandbox{}, false
	}
	return sb, true
}

// sandboxStatus summarizes a live Sandbox object's Ready condition.
func sandboxStatus(obj *unstructured.Unstructured) string {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conditions {
		cond, _ := c.(map[string]any)
		if cond["type"] == "Ready" && cond["status"] == "True" {
			return statusReady
		}
	}
	return statusNotReady
}

// sandboxNodeSelector pins sandbox pods to matching nodes, from
// SANDBOX_NODE_SELECTOR ("key=value,key2=value2"; unset means anywhere). The
// benchmark instance uses it to keep sandboxes on the benchmark node group.
var sandboxNodeSelector = parseNodeSelector(os.Getenv("SANDBOX_NODE_SELECTOR"))

func parseNodeSelector(s string) map[string]string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || k == "" {
			log.Fatalf("SANDBOX_NODE_SELECTOR: %q is not key=value", pair)
		}
		out[k] = v
	}
	return out
}

// podSpec validates the request and builds the single-container pod spec.
func (req sandboxRequest) podSpec() (corev1.PodSpec, error) {
	image := strings.TrimSpace(req.Image)
	if image == "" || strings.ContainsAny(image, " \t\n") {
		return corev1.PodSpec{}, errors.New("image is required and must not contain whitespace")
	}

	names := make([]string, 0, len(req.Env))
	for name := range req.Env {
		if errs := validation.IsEnvVarName(name); len(errs) > 0 {
			return corev1.PodSpec{}, fmt.Errorf("env %q: %s", name, strings.Join(errs, "; "))
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var env []corev1.EnvVar
	for _, name := range names {
		env = append(env, corev1.EnvVar{Name: name, Value: req.Env[name]})
	}

	requests, err := resourceList("requests", req.Resources.Requests)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	limits, err := resourceList("limits", req.Resources.Limits)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	for name, req := range requests {
		if lim, ok := limits[name]; ok && req.Cmp(lim) > 0 {
			return corev1.PodSpec{}, fmt.Errorf("resources: %s request %s exceeds limit %s", name, req.String(), lim.String())
		}
	}

	return corev1.PodSpec{
		TerminationGracePeriodSeconds: &sandboxGracePeriod,
		// Sandboxes get no Kubernetes credentials, and no env vars describing
		// other services in the namespace. Network isolation is the
		// sandbox-isolation NetworkPolicy in k8s/sandboxes.yaml.
		AutomountServiceAccountToken: &noAutomount,
		EnableServiceLinks:           &noServiceLinks,
		NodeSelector:                 sandboxNodeSelector,
		Containers: []corev1.Container{{
			Name:    userContainerName,
			Image:   image,
			Command: req.Command,
			Args:    req.Args,
			Env:     env,
			Resources: corev1.ResourceRequirements{
				Requests: requests,
				Limits:   limits,
			},
		}},
	}, nil
}

func resourceList(field string, in map[string]string) (corev1.ResourceList, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := corev1.ResourceList{}
	for name, value := range in {
		if errs := validation.IsQualifiedName(name); len(errs) > 0 {
			return nil, fmt.Errorf("resources.%s: invalid resource name %q", field, name)
		}
		q, err := resource.ParseQuantity(value)
		if err != nil {
			return nil, fmt.Errorf("resources.%s.%s: invalid quantity %q", field, name, value)
		}
		out[corev1.ResourceName(name)] = q
	}
	return out, nil
}

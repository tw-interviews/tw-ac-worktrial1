package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sandbox-manager/internal/workspace"
)

// Every sandbox's /workspace is an emptyDir on its node. The node's
// sandbox-agent (agent/, a DaemonSet) does all file I/O on it: the API
// forwards file requests for a running sandbox to the agent on its pod's
// node (agent.go).
//
// The workspace only lives as long as the pod, so it's saved to S3 as a
// snapshot (a tar, see snapshots.go) when the sandbox is paused, deleted or
// about to expire, and restored into the new pod on resume. While a sandbox
// is paused, or gone (for the recovery window), file requests are served
// from its latest snapshot instead; writes to a paused sandbox rewrite it.
//
// Pods that must not start their user container yet (files being staged on
// create, or a snapshot to restore) get a "wait" init container that holds
// the pod until the agent marks it ready (withGate).
const (
	defaultVolumeSize = "1Gi"
	// Volume names the agent looks for; see agent/main.go.
	workspaceVolume    = "sandbox-workspace"
	controlVolume      = "sandbox-control"
	waitContainerName  = "wait"
	waitImage          = "busybox:1.36"
	workspaceMountPath = "/workspace"

	// How long a file request waits for the sandbox's pod to be ready for it.
	filesPodWait   = 90 * time.Second
	maxUploadBytes = 10 << 30
)

// withWorkspace adds the workspace emptyDir, mounted at /workspace in the
// user container. With gated, the pod also waits for the agent (withGate).
func withWorkspace(spec corev1.PodSpec, gated bool) corev1.PodSpec {
	spec = copySpec(spec)
	size := resource.MustParse(defaultVolumeSize)
	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name:         workspaceVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &size}},
	})
	for i := range spec.Containers {
		if spec.Containers[i].Name == userContainerName {
			spec.Containers[i].VolumeMounts = append(spec.Containers[i].VolumeMounts, corev1.VolumeMount{
				Name: workspaceVolume, MountPath: workspaceMountPath,
			})
		}
	}
	if gated {
		spec = withGate(spec)
	}
	return spec
}

// withGate makes the pod's user container wait until the agent marks the pod
// ready (after staging, or after restoring a snapshot): a locked-down init
// container polls for the agent's marker in a small volume that only it and
// the agent see. Idempotent.
func withGate(spec corev1.PodSpec) corev1.PodSpec {
	for _, c := range spec.InitContainers {
		if c.Name == waitContainerName {
			return spec
		}
	}
	spec = copySpec(spec)
	size := resource.MustParse("1Mi")
	spec.Volumes = append(spec.Volumes, corev1.Volume{
		// On disk, not tmpfs: the agent sees it through the kubelet's pods
		// directory, which wouldn't show mounts made after the agent started.
		Name:         controlVolume,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &size}},
	})
	nobody, yes, no := int64(65534), true, false
	spec.InitContainers = append(spec.InitContainers, corev1.Container{
		Name:         waitContainerName,
		Image:        waitImage,
		Command:      []string{"sh", "-c", "until [ -e /control/ready ]; do sleep 0.2; done"},
		VolumeMounts: []corev1.VolumeMount{{Name: controlVolume, MountPath: "/control", ReadOnly: true}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("5m"), corev1.ResourceMemory: resource.MustParse("4Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                &nobody,
			RunAsNonRoot:             &yes,
			ReadOnlyRootFilesystem:   &yes,
			AllowPrivilegeEscalation: &no,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	})
	return spec
}

// copySpec copies the slices withWorkspace/withGate modify, so the caller's
// spec is left untouched.
func copySpec(spec corev1.PodSpec) corev1.PodSpec {
	spec.Volumes = append([]corev1.Volume(nil), spec.Volumes...)
	spec.InitContainers = append([]corev1.Container(nil), spec.InitContainers...)
	spec.Containers = append([]corev1.Container(nil), spec.Containers...)
	for i := range spec.Containers {
		spec.Containers[i].VolumeMounts = append([]corev1.VolumeMount(nil), spec.Containers[i].VolumeMounts...)
	}
	return spec
}

// deleteLegacyVolume deletes the PVC (and recovery pod, if any) of a sandbox
// created when sandboxes had persistent volumes. Missing objects are fine.
func (s *server) deleteLegacyVolume(ctx context.Context, sb sandbox) error {
	if sb.Volume == nil {
		return nil
	}
	err := s.core.CoreV1().Pods(sb.Namespace).Delete(ctx, "rcv-"+sb.SandboxID, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	err = s.core.CoreV1().PersistentVolumeClaims(sb.Namespace).Delete(ctx, *sb.Volume, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// sandboxPodOf returns the Sandbox's current (not terminating) pod, found
// via its status.selector and controller owner, or nil if it has none.
func (s *server) sandboxPodOf(ctx context.Context, sb sandbox, obj *unstructured.Unstructured) (*corev1.Pod, error) {
	selector, _, _ := unstructured.NestedString(obj.Object, "status", "selector")
	if selector == "" {
		return nil, nil
	}
	pods, err := s.core.CoreV1().Pods(sb.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOf(pod)
		if owner != nil && owner.UID == obj.GetUID() && pod.DeletionTimestamp == nil {
			return pod, nil
		}
	}
	return nil, nil
}

// waitingForAgent reports whether the pod's wait init container is running,
// i.e. its volumes exist and it waits for staging or a restore.
func waitingForAgent(pod *corev1.Pod) bool {
	for _, st := range pod.Status.InitContainerStatuses {
		if st.Name == waitContainerName {
			return st.State.Running != nil
		}
	}
	return false
}

// initialized reports whether the pod is past its init containers, so its
// workspace holds the sandbox's files (staged or restored).
func initialized(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodInitialized {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// livePod waits up to filesPodWait for the running sandbox's pod to be
// ready for file requests: initialized, or (with staging) waiting for the
// staged files. gone=true means the Sandbox is gone, going or paused, so
// its files are only in its snapshot.
func (s *server) livePod(ctx context.Context, sb sandbox, staging bool) (pod *corev1.Pod, gone bool, status int, msg string) {
	ctx, cancel := context.WithTimeout(ctx, filesPodWait)
	defer cancel()
	for {
		obj, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Get(ctx, sb.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err) || (err == nil && (obj.GetDeletionTimestamp() != nil || specMode(obj) == modeSuspended)):
			return nil, true, 0, ""
		case err != nil:
			log.Printf("get sandbox %s: %v", sb.Name, err)
			return nil, false, http.StatusBadGateway, "failed to read sandbox from cluster"
		}
		pod, err := s.sandboxPodOf(ctx, sb, obj)
		if err != nil {
			log.Printf("find pod of sandbox %s: %v", sb.Name, err)
			return nil, false, http.StatusBadGateway, "failed to read sandbox from cluster"
		}
		if pod != nil && (initialized(pod) || (staging && waitingForAgent(pod))) {
			return pod, false, 0, ""
		}
		select {
		case <-ctx.Done():
			return nil, false, http.StatusConflict, "sandbox's pod hasn't started yet; retry shortly"
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// fileTarget is where a file request is served from: the live pod (through
// its node's agent), or the snapshot when pod is nil.
type fileTarget struct {
	sb  sandbox
	pod *corev1.Pod
	// tx (writes only) holds the sandbox's row lock until the write is done:
	// FOR KEY SHARE for a live write, so a pause or delete (FOR UPDATE)
	// waits for it and its snapshot includes it; FOR UPDATE for a write to
	// a paused sandbox's snapshot. The caller rolls it back (or commits).
	tx pgx.Tx
}

// fileTarget loads the {id} sandbox and decides where to serve the request
// from, writing an error response and returning ok=false if it can't be.
// Writes to gone sandboxes are refused.
func (s *server) fileTarget(w http.ResponseWriter, r *http.Request, write bool) (t fileTarget, ok bool) {
	ctx := r.Context()
	sb, ok := s.loadSandbox(w, r, r.PathValue("id"))
	if !ok {
		return t, false
	}
	if !sb.usesSnapshots() {
		writeError(w, http.StatusConflict, "sandbox was created by an older version and its files can't be accessed; delete it")
		return t, false
	}
	if write {
		tx, err := s.db.Begin(ctx)
		if err != nil {
			s.internalError(w, err)
			return t, false
		}
		lock := "FOR KEY SHARE"
		if sb.operatingMode == modeSuspended {
			lock = "FOR UPDATE"
		}
		sb, err = scanSandbox(tx.QueryRow(ctx, `SELECT `+sandboxColumns+` FROM sandboxes WHERE sandbox_id = $1 `+lock, sb.SandboxID))
		if err == nil && sb.operatingMode == modeSuspended && lock != "FOR UPDATE" {
			// Paused while we waited for the lock.
			sb, err = scanSandbox(tx.QueryRow(ctx, `SELECT `+sandboxColumns+` FROM sandboxes WHERE sandbox_id = $1 FOR UPDATE`, sb.SandboxID))
		}
		if err != nil {
			tx.Rollback(ctx)
			s.internalError(w, err)
			return t, false
		}
		t.tx = tx
	}
	fail := func(status int, msg string) (fileTarget, bool) {
		if t.tx != nil {
			t.tx.Rollback(ctx)
		}
		writeError(w, status, msg)
		return fileTarget{}, false
	}
	t.sb = sb

	switch {
	case sb.gone() && write:
		return fail(http.StatusConflict, msgSandboxGone+"; its files are read-only")
	case sb.gone():
		return t, s.checkRetained(w, sb)
	case sb.operatingMode == modeSuspended:
		return t, true
	}
	pod, gone, status, msg := s.livePod(ctx, sb, sb.stageFiles && sb.stagedAt == nil)
	switch {
	case gone && write:
		return fail(http.StatusConflict, msgSandboxGone+"; its files are read-only")
	case gone:
		return t, s.checkRetained(w, sb)
	case pod == nil:
		return fail(status, msg)
	}
	t.pod = pod
	return t, true
}

// checkRetained writes 410 if a gone sandbox's files are no longer kept.
func (s *server) checkRetained(w http.ResponseWriter, sb sandbox) bool {
	if until := sb.retainedUntil(s.volumeRetention); until == nil || !time.Now().Before(*until) {
		writeError(w, http.StatusGone, "sandbox files are no longer available (none were saved, or the recovery window is over)")
		return false
	}
	return true
}

// keepAlive refreshes a running sandbox's TTL now and every 30s until the
// returned stop is called, so long transfers count as activity and the
// sandbox can't expire mid-transfer. Paused sandboxes keep their paused TTL.
//
// Refreshes run in the background, never in the caller: a live write holds
// the row's KEY SHARE lock, and if a pause is already queued for the row, a
// refresh's UPDATE queues behind the pause, which waits for the write.
func (s *server) keepAlive(ctx context.Context, sb sandbox) (stop func()) {
	if sb.gone() || sb.TTLSeconds == nil || sb.operatingMode == modeSuspended {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	refresh := func() {
		_, err := s.extendTTL(ctx, sb, 0)
		if err != nil && ctx.Err() == nil && !apierrors.IsNotFound(err) && !errors.Is(err, errSandboxPaused) {
			log.Printf("refresh ttl of sandbox %s: %v", sb.Name, err)
		}
	}
	go func() {
		refresh()
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refresh()
			}
		}
	}()
	return cancel
}

// writeWorkspaceError maps internal/workspace errors to responses.
func (s *server) writeWorkspaceError(w http.ResponseWriter, err error, what string) {
	var bad *workspace.BadEntryError
	var tooBig *http.MaxBytesError
	switch {
	case errors.Is(err, workspace.ErrNotFound):
		writeError(w, http.StatusNotFound, "no such file or directory")
	case errors.Is(err, workspace.ErrNotDir):
		writeError(w, http.StatusBadRequest, "not a directory")
	case errors.Is(err, workspace.ErrEscapes):
		writeError(w, http.StatusBadRequest, "path escapes the workspace")
	case errors.Is(err, workspace.ErrIsDir):
		writeError(w, http.StatusConflict, "path is a directory")
	case errors.Is(err, workspace.ErrSymlink):
		writeError(w, http.StatusConflict, err.Error())
	case errors.As(err, &bad):
		writeError(w, http.StatusBadRequest, bad.Error())
	case errors.As(err, &tooBig):
		writeError(w, http.StatusRequestEntityTooLarge, "upload too large")
	default:
		log.Printf("%s: %v", what, err)
		writeError(w, http.StatusBadGateway, what+" failed")
	}
}

// downloadName is the filename offered for a download of rel.
func downloadName(rel string, dir bool) string {
	base := path.Base("/" + rel)
	if rel == "" {
		base = "workspace"
	}
	if dir {
		base += ".tar"
	}
	return base
}

// uploadTar extracts a tar stream (the request body) into the workspace:
// POST /sandboxes/{id}/files. Entry names are relative to /workspace. Only
// regular files and directories are accepted. Responds with the number of
// files and bytes written.
func (s *server) uploadTar(w http.ResponseWriter, r *http.Request) {
	t, ok := s.fileTarget(w, r, true)
	if !ok {
		return
	}
	defer t.tx.Rollback(r.Context())
	body := http.MaxBytesReader(w, r.Body, maxUploadBytes)
	if t.pod != nil {
		defer s.keepAlive(r.Context(), t.sb)()
		s.proxyFile(w, r, t.pod, "", body)
		return
	}

	// Paused: the new snapshot is the old one minus the files being
	// replaced, plus the upload. The upload is parked in S3 first, since its
	// names must be known before the old entries are copied.
	ctx := r.Context()
	tmp := s.store.NewKey(t.sb.SandboxID) + ".upload"
	if _, err := s.store.Put(ctx, tmp, body); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "upload too large")
			return
		}
		log.Printf("park upload for sandbox %s: %v", t.sb.SandboxID, err)
		writeError(w, http.StatusBadGateway, "upload failed")
		return
	}
	defer s.store.Delete(context.WithoutCancel(ctx), tmp)
	rc, err := s.store.Get(ctx, tmp)
	if err != nil {
		writeError(w, http.StatusBadGateway, "upload failed")
		return
	}
	names, err := workspace.UploadNames(rc, "")
	rc.Close()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tar stream: "+err.Error())
		return
	}
	var files, n int64
	oldKey, err := s.rewriteSnapshot(ctx, t.tx, &t.sb, func(name string) bool { return names[name] },
		func(tw *tar.Writer, types map[string]byte) error {
			rc, err := s.store.Get(ctx, tmp)
			if err != nil {
				return err
			}
			defer rc.Close()
			files, n, err = workspace.AddUpload(tw, types, "", rc)
			return err
		})
	if err != nil {
		s.writeWorkspaceError(w, err, "upload")
		return
	}
	if !s.commitRewrite(w, r, t.tx, oldKey) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"files": files, "bytes": n})
}

// putFile streams the request body into one file:
// PUT /sandboxes/{id}/files/{path}. Readers never see a partial file, and
// parent directories are created. Responds with the path, size and SHA-256.
// Writing to a paused sandbox needs a Content-Length.
func (s *server) putFile(w http.ResponseWriter, r *http.Request) {
	rel := workspace.Rel(r.PathValue("path"))
	if rel == "" {
		writeError(w, http.StatusBadRequest, "a file path is required")
		return
	}
	t, ok := s.fileTarget(w, r, true)
	if !ok {
		return
	}
	defer t.tx.Rollback(r.Context())
	body := http.MaxBytesReader(w, r.Body, maxUploadBytes)
	if t.pod != nil {
		defer s.keepAlive(r.Context(), t.sb)()
		s.proxyFile(w, r, t.pod, rel, body)
		return
	}

	if r.ContentLength < 0 {
		writeError(w, http.StatusLengthRequired, "writing to a paused sandbox needs a Content-Length")
		return
	}
	var sum string
	oldKey, err := s.rewriteSnapshot(r.Context(), t.tx, &t.sb, func(name string) bool { return name == rel },
		func(tw *tar.Writer, types map[string]byte) error {
			var err error
			sum, err = workspace.AddFile(tw, types, rel, r.ContentLength, body)
			return err
		})
	if err != nil {
		s.writeWorkspaceError(w, err, "upload")
		return
	}
	if !s.commitRewrite(w, r, t.tx, oldKey) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": workspaceMountPath + "/" + rel, "bytes": r.ContentLength, "sha256": sum})
}

// getFile streams a file, or a directory as a tar stream:
// GET /sandboxes/{id}/files/{path}. With ?list it returns the directory's
// entries as JSON instead. Works for gone sandboxes during the recovery
// window.
func (s *server) getFile(w http.ResponseWriter, r *http.Request) {
	rel := workspace.Rel(r.PathValue("path"))
	t, ok := s.fileTarget(w, r, false)
	if !ok {
		return
	}
	if t.pod != nil {
		defer s.keepAlive(r.Context(), t.sb)()
		s.proxyFile(w, r, t.pod, rel, nil)
		return
	}

	snap, err := s.openSnapshot(r.Context(), t.sb)
	if err != nil {
		log.Printf("read snapshot of sandbox %s: %v", t.sb.SandboxID, err)
		writeError(w, http.StatusBadGateway, "failed to read sandbox files")
		return
	}
	defer snap.Close()
	if _, list := r.URL.Query()["list"]; list {
		entries, err := workspace.ListSnapshot(snap, rel)
		if err != nil {
			s.writeWorkspaceError(w, err, "list")
			return
		}
		writeJSON(w, http.StatusOK, entries)
		return
	}
	started := false
	err = workspace.ReadSnapshot(snap, rel, func(size int64, body io.Reader) error {
		started = true
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", downloadName(rel, false)))
		w.WriteHeader(http.StatusOK)
		_, err := io.Copy(w, body)
		return err
	}, func() (io.Writer, error) {
		started = true
		w.Header().Set("Content-Type", "application/x-tar")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", downloadName(rel, true)))
		w.WriteHeader(http.StatusOK)
		return w, nil
	})
	switch {
	case err != nil && !started:
		s.writeWorkspaceError(w, err, "download")
	case err != nil && r.Context().Err() == nil:
		log.Printf("download %s from snapshot of sandbox %s: %v", rel, t.sb.SandboxID, err)
		panic(http.ErrAbortHandler) // headers are sent; cut the response short
	}
}

// deleteFile removes a file or directory (recursively):
// DELETE /sandboxes/{id}/files/{path}.
func (s *server) deleteFile(w http.ResponseWriter, r *http.Request) {
	rel := workspace.Rel(r.PathValue("path"))
	if rel == "" {
		writeError(w, http.StatusBadRequest, "refusing to delete the whole workspace")
		return
	}
	t, ok := s.fileTarget(w, r, true)
	if !ok {
		return
	}
	defer t.tx.Rollback(r.Context())
	if t.pod != nil {
		defer s.keepAlive(r.Context(), t.sb)()
		s.proxyFile(w, r, t.pod, rel, nil)
		return
	}

	found := false
	oldKey, err := s.rewriteSnapshot(r.Context(), t.tx, &t.sb, func(name string) bool {
		if workspace.IsBelow(name, rel) {
			found = true
			return true
		}
		return false
	}, func(*tar.Writer, map[string]byte) error {
		if !found {
			return workspace.ErrNotFound
		}
		return nil
	})
	if err != nil {
		s.writeWorkspaceError(w, err, "delete")
		return
	}
	if !s.commitRewrite(w, r, t.tx, oldKey) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// proxyFile forwards a file request for a live sandbox to the agent on its
// pod's node and relays the response.
func (s *server) proxyFile(w http.ResponseWriter, r *http.Request, pod *corev1.Pod, rel string, body io.Reader) {
	p := "/files"
	if rel != "" {
		p += "/" + rel
	}
	size := int64(-1)
	if body != nil {
		size = r.ContentLength
	}
	resp, err := s.agent.do(r.Context(), pod, r.Method, p, r.URL.RawQuery, body, size)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "upload too large")
			return
		}
		log.Printf("agent %s %s for pod %s: %v", r.Method, p, pod.Name, err)
		writeError(w, http.StatusBadGateway, "failed to reach the sandbox's node agent")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		var e struct{ Code string }
		if json.Unmarshal(msg, &e) == nil && e.Code == "no_workspace" {
			writeError(w, http.StatusConflict, "sandbox's pod went away; retry shortly")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		w.Write(msg)
		return
	}
	for _, h := range []string{"Content-Type", "Content-Length"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	if _, list := r.URL.Query()["list"]; r.Method == http.MethodGet && !list {
		dir := resp.Header.Get("Content-Type") == "application/x-tar"
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", downloadName(rel, dir)))
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil && r.Context().Err() == nil {
		log.Printf("relay %s %s for pod %s: %v", r.Method, p, pod.Name, err)
		panic(http.ErrAbortHandler)
	}
}

// completeStaging lets a sandbox created with stage_files start its user
// container: POST /sandboxes/{id}/staging/complete. Idempotent.
func (s *server) completeStaging(w http.ResponseWriter, r *http.Request) {
	sb, ok := s.loadSandbox(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	if sb.gone() {
		writeError(w, http.StatusConflict, msgSandboxGone)
		return
	}
	if sb.stagedAt == nil {
		pod, gone, status, msg := s.livePod(r.Context(), sb, true)
		switch {
		case gone:
			writeError(w, http.StatusConflict, msgSandboxGone)
			return
		case pod == nil:
			writeError(w, status, msg)
			return
		}
		if waitingForAgent(pod) {
			if err := s.agent.call(r.Context(), pod, http.MethodPost, "/ready", nil, nil); err != nil {
				log.Printf("mark pod %s ready: %v", pod.Name, err)
				writeError(w, http.StatusBadGateway, "failed to complete staging")
				return
			}
		}
	}
	if _, err := s.db.Exec(r.Context(), `UPDATE sandboxes SET staged_at = COALESCE(staged_at, now()) WHERE sandbox_id = $1`, sb.SandboxID); err != nil {
		s.internalError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// openSnapshot streams the sandbox's latest snapshot (an empty workspace if
// it has none).
func (s *server) openSnapshot(ctx context.Context, sb sandbox) (io.ReadCloser, error) {
	if sb.snapshotKey == nil {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return s.store.Get(ctx, *sb.snapshotKey)
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/client-go/util/exec"
)

const (
	defaultExecTimeout = 30 * time.Second
	maxExecTimeout     = 5 * time.Minute
	maxExecOutputBytes = 1 << 20 // per stream
)

// Execution statuses. "completed" means the command ran to the end, whatever
// its exit code; "error" means the exec stream itself failed.
const (
	execRunning   = "running"
	execCompleted = "completed"
	execTimedOut  = "timed_out"
	execError     = "error"
)

type executionRequest struct {
	SandboxID      string   `json:"sandbox_id"`
	Command        []string `json:"command"`
	Stdin          string   `json:"stdin,omitempty"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`
}

// execution is one command run in a sandbox's user container. A sandbox can
// have any number of executions, including several running at once.
type execution struct {
	ExecutionID     string     `json:"execution_id"`
	SandboxID       string     `json:"sandbox_id"`
	Pod             string     `json:"pod"`
	Command         []string   `json:"command"`
	Status          string     `json:"status"`
	ExitCode        *int       `json:"exit_code"`
	Stdout          string     `json:"stdout"`
	Stderr          string     `json:"stderr"`
	StdoutTruncated bool       `json:"stdout_truncated"`
	StderrTruncated bool       `json:"stderr_truncated"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	// SandboxExpiresAt is the sandbox's TTL expiry after this execution
	// refreshed it. Only set on the POST response; not stored.
	SandboxExpiresAt *time.Time `json:"sandbox_expires_at,omitempty"`
}

const executionColumns = `execution_id::text, sandbox_id::text, pod, command, status, exit_code,
	stdout, stderr, stdout_truncated, stderr_truncated, started_at, finished_at`

func scanExecution(row pgx.Row) (execution, error) {
	var e execution
	err := row.Scan(&e.ExecutionID, &e.SandboxID, &e.Pod, &e.Command, &e.Status, &e.ExitCode,
		&e.Stdout, &e.Stderr, &e.StdoutTruncated, &e.StderrTruncated, &e.StartedAt, &e.FinishedAt)
	return e, err
}

// createExecution runs a command in the sandbox's user container, records it,
// and waits for it to finish. The row is written as "running" first so the
// execution is visible via GET while it runs. A non-zero exit code is still a
// 200: the execution happened, and the caller reads exit_code.
func (s *server) createExecution(w http.ResponseWriter, r *http.Request) {
	var req executionRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Command) == 0 || req.Command[0] == "" {
		writeError(w, http.StatusBadRequest, "command is required")
		return
	}
	timeout := defaultExecTimeout
	if req.TimeoutSeconds != 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
		if timeout < 0 || timeout > maxExecTimeout {
			writeError(w, http.StatusBadRequest, "timeout_seconds must be between 1 and 300")
			return
		}
	}

	sb, ok := s.loadSandbox(w, r, req.SandboxID)
	if !ok {
		return
	}
	if sb.operatingMode == modeSuspended {
		writeErrorCode(w, http.StatusConflict, codeSandboxPaused, msgSandboxPaused)
		return
	}
	pod, status, msg := s.sandboxPod(r.Context(), sb)
	if pod == nil {
		writeError(w, status, msg)
		return
	}

	// Refresh the TTL and record the execution as running in one
	// transaction. Its row lock is the one pauseSandbox takes, so a pause
	// either sees this execution running or happens first and this fails.
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		s.internalError(w, err)
		return
	}
	defer tx.Rollback(r.Context())
	// Every command refreshes the sandbox's TTL. Cover the command's whole
	// timeout so the sandbox can't expire while it runs.
	expiresAt, err := s.extendTTLTx(r.Context(), tx, sb, timeout)
	switch {
	case errors.Is(err, errSandboxPaused):
		writeErrorCode(w, http.StatusConflict, codeSandboxPaused, msgSandboxPaused)
		return
	case apierrors.IsNotFound(err):
		writeError(w, http.StatusConflict, msgSandboxGone)
		return
	case err != nil:
		log.Printf("extend ttl of sandbox %s: %v", sb.Name, err)
		writeError(w, http.StatusBadGateway, "failed to refresh sandbox ttl")
		return
	}

	e := execution{
		ExecutionID: uuid.NewString(),
		SandboxID:   sb.SandboxID,
		Pod:         pod.Name,
		Command:     req.Command,
		Status:      execRunning,

		SandboxExpiresAt: expiresAt,
	}
	err = tx.QueryRow(r.Context(),
		`INSERT INTO executions (execution_id, sandbox_id, pod, command, status) VALUES ($1, $2, $3, $4, $5) RETURNING started_at`,
		e.ExecutionID, e.SandboxID, e.Pod, e.Command, e.Status,
	).Scan(&e.StartedAt)
	if err == nil {
		err = tx.Commit(r.Context())
	}
	if err != nil {
		s.internalError(w, err)
		return
	}

	execErr := s.runInUserContainer(r.Context(), pod, req, timeout, &e)

	// Record the outcome even if the client has gone away mid-execution.
	err = s.db.QueryRow(context.WithoutCancel(r.Context()),
		`UPDATE executions SET status = $2, exit_code = $3, stdout = $4, stderr = $5,
			stdout_truncated = $6, stderr_truncated = $7, finished_at = now()
		WHERE execution_id = $1 RETURNING finished_at`,
		e.ExecutionID, e.Status, e.ExitCode, e.Stdout, e.Stderr, e.StdoutTruncated, e.StderrTruncated,
	).Scan(&e.FinishedAt)
	if err != nil {
		s.internalError(w, err)
		return
	}
	if execErr != nil {
		log.Printf("exec %s in %s/%s: %v", e.ExecutionID, pod.Namespace, pod.Name, execErr)
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error":        "failed to execute command in sandbox",
			"execution_id": e.ExecutionID,
		})
		return
	}
	writeJSON(w, http.StatusOK, e)
}

// runInUserContainer execs req.Command in the pod's user container and fills
// in e's status, exit code and output. It returns an error only when the exec
// stream itself fails (e.Status is then "error").
func (s *server) runInUserContainer(ctx context.Context, pod *corev1.Pod, req executionRequest, timeout time.Duration, e *execution) error {
	stdout := &cappedBuffer{limit: maxExecOutputBytes}
	stderr := &cappedBuffer{limit: maxExecOutputBytes}
	var stdin io.Reader
	if req.Stdin != "" {
		stdin = strings.NewReader(req.Stdin)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := s.execIn(ctx, pod, userContainerName, req.Command, stdin, stdout, stderr)

	e.Stdout, e.StdoutTruncated = stdout.buf.String(), stdout.truncated
	e.Stderr, e.StderrTruncated = stderr.buf.String(), stderr.truncated
	var exitErr utilexec.ExitError
	switch {
	case err == nil:
		e.Status = execCompleted
		e.ExitCode = new(int)
	case errors.As(err, &exitErr):
		e.Status = execCompleted
		code := exitErr.ExitStatus()
		e.ExitCode = &code
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		// The stream is closed, but the process may keep running in the container.
		e.Status = execTimedOut
	default:
		e.Status = execError
		return err
	}
	return nil
}

// listExecutions returns executions newest first, optionally filtered with
// ?sandbox_id=. Output is omitted from the list; fetch one execution for it.
func (s *server) listExecutions(w http.ResponseWriter, r *http.Request) {
	query := `SELECT ` + executionColumns + ` FROM executions`
	var args []any
	if raw := r.URL.Query().Get("sandbox_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "sandbox_id must be a UUID")
			return
		}
		query += ` WHERE sandbox_id = $1`
		args = append(args, id)
	}
	rows, err := s.db.Query(r.Context(), query+` ORDER BY started_at DESC`, args...)
	if err != nil {
		s.internalError(w, err)
		return
	}
	executions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (execution, error) {
		e, err := scanExecution(row)
		e.Stdout, e.Stderr = "", ""
		return e, err
	})
	if err != nil {
		s.internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, executions)
}

func (s *server) getExecution(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "execution id must be a UUID")
		return
	}
	e, err := scanExecution(s.db.QueryRow(r.Context(), `SELECT `+executionColumns+` FROM executions WHERE execution_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	if err != nil {
		s.internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}

// execIn runs command in one of a pod's containers, streaming stdin (if not
// nil) in and stdout/stderr out, and returns once it exits. A non-zero exit
// comes back as a utilexec.ExitError.
func (s *server) execIn(ctx context.Context, pod *corev1.Pod, container string, command []string, stdin io.Reader, stdout, stderr io.Writer) error {
	execReq := s.core.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	executor, err := s.newExecutor(execReq.URL())
	if err != nil {
		return err
	}
	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: stdin, Stdout: stdout, Stderr: stderr})
}

// newExecutor prefers the WebSocket exec protocol and falls back to SPDY for
// API servers that don't support it, the same way kubectl does.
func (s *server) newExecutor(u *url.URL) (remotecommand.Executor, error) {
	ws, err := remotecommand.NewWebSocketExecutor(s.restConfig, "GET", u.String())
	if err != nil {
		return nil, err
	}
	spdy, err := remotecommand.NewSPDYExecutor(s.restConfig, "POST", u)
	if err != nil {
		return nil, err
	}
	return remotecommand.NewFallbackExecutor(ws, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
}

// sandboxPod finds the running pod that the agent-sandbox controller created
// for sb, via the Sandbox's status.selector and the pod's owner reference.
// On failure it returns a nil pod plus the HTTP status and message to send.
func (s *server) sandboxPod(ctx context.Context, sb sandbox) (*corev1.Pod, int, string) {
	obj, err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Get(ctx, sb.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, http.StatusConflict, msgSandboxGone
	}
	if err != nil {
		log.Printf("get sandbox %s: %v", sb.Name, err)
		return nil, http.StatusBadGateway, "failed to read sandbox from cluster"
	}
	if obj.GetDeletionTimestamp() != nil {
		return nil, http.StatusConflict, "sandbox is being deleted"
	}
	if specMode(obj) == modeSuspended {
		return nil, http.StatusConflict, msgSandboxPaused
	}
	if sandboxStatus(obj) != statusReady {
		return nil, http.StatusConflict, "sandbox is not ready"
	}
	selector, _, _ := unstructured.NestedString(obj.Object, "status", "selector")
	if selector == "" {
		return nil, http.StatusConflict, "sandbox has no pod yet"
	}

	pods, err := s.core.CoreV1().Pods(sb.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		log.Printf("list pods for sandbox %s: %v", sb.Name, err)
		return nil, http.StatusBadGateway, "failed to read sandbox pod from cluster"
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.UID != obj.GetUID() || pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
			continue
		}
		for _, c := range pod.Spec.Containers {
			if c.Name == userContainerName {
				return pod, 0, ""
			}
		}
		return nil, http.StatusConflict, "sandbox pod has no user container"
	}
	return nil, http.StatusConflict, "sandbox has no running pod"
}

// cappedBuffer keeps the first limit bytes written to it and discards the
// rest, so a chatty command can't exhaust the server's memory.
type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.limit - c.buf.Len(); len(p) > room {
		c.truncated = true
		if room > 0 {
			c.buf.Write(p[:room])
		}
		return len(p), nil
	}
	return c.buf.Write(p)
}

// Command toolcall-runtime-minimal is the minimal sandbox runtime for the tool-call
// benchmark: the same protocol as ../python/server.py (the upstream SDK's
// "legacy python-runtime" /execute), as one static binary on busybox, so it is
// listening within milliseconds instead of the seconds Python + FastAPI take
// to start under a 250m CPU limit.
//
//	POST /execute  {"command": "...", "timeout_seconds": 1.5}
//	            -> {"stdout": "...", "stderr": "...", "exit_code": 0, "timed_out": false}
//	GET /          readiness
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os/exec"
	"time"
)

type executeRequest struct {
	Command        string   `json:"command"`
	TimeoutSeconds *float64 `json:"timeout_seconds"`
}

type executionResult struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
	TimedOut bool   `json:"timed_out"`
}

func main() {
	http.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})
	http.HandleFunc("POST /execute", execute)
	log.Fatal(http.ListenAndServe(":8888", nil))
}

func execute(w http.ResponseWriter, r *http.Request) {
	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if req.TimeoutSeconds != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*req.TimeoutSeconds*float64(time.Second)))
		defer cancel()
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", req.Command)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()

	res := executionResult{Stdout: stdout.String(), Stderr: stderr.String()}
	var exitErr *exec.ExitError
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		res.ExitCode, res.TimedOut = 124, true
	case err == nil:
	case errors.As(err, &exitErr):
		res.ExitCode = exitErr.ExitCode()
	default:
		res.ExitCode = -1
		res.Stderr += err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

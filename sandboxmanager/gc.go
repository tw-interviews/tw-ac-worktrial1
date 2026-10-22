package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// gcLockKey identifies the Postgres advisory lock that makes only one
// replica run a GC pass at a time.
const gcLockKey = 0x5ADB0C5

// runGC periodically cleans up after gone sandboxes until ctx is done.
func (s *server) runGC(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		s.gcOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// gcOnce does one pass: sandboxes gone (deleted, or expired) for longer
// than the recovery window get their Sandbox object deleted if it somehow
// remains, then their snapshots (or legacy volume), then the row (and with
// it, its executions).
func (s *server) gcOnce(ctx context.Context) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		log.Printf("gc: %v", err)
		return
	}
	defer tx.Rollback(ctx)
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, gcLockKey).Scan(&locked); err != nil || !locked {
		return // another replica is collecting
	}

	cutoff := time.Now().Add(-s.volumeRetention)
	rows, err := tx.Query(ctx, `SELECT `+sandboxColumns+` FROM sandboxes
		WHERE deleted_at < $1 OR expires_at < $1`, cutoff)
	if err != nil {
		log.Printf("gc: %v", err)
		return
	}
	expired, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sandbox, error) { return scanSandbox(row) })
	if err != nil {
		log.Printf("gc: %v", err)
		return
	}
	for _, sb := range expired {
		err := s.kube.Resource(sandboxGVR).Namespace(sb.Namespace).Delete(ctx, sb.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			log.Printf("gc: delete sandbox %s: %v", sb.Name, err)
			continue
		}
		if err := s.deleteFiles(ctx, sb); err != nil {
			log.Printf("gc: delete files of sandbox %s: %v", sb.Name, err)
			continue
		}
		if _, err := tx.Exec(ctx, `DELETE FROM sandboxes WHERE sandbox_id = $1`, sb.SandboxID); err != nil {
			log.Printf("gc: delete sandbox row %s: %v", sb.SandboxID, err)
			return
		}
		log.Printf("gc: recovery window over, deleted sandbox %s and its files", sb.SandboxID)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("gc: %v", err)
	}
}

package postgres

import (
	"context"
	"fmt"
	"io"
	"os/exec"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CopySpec describes a single table's data to copy via a SELECT query.
// Query must be a valid SELECT statement (e.g. "SELECT * FROM parts WHERE project_id = 42").
// Table is the destination table name for COPY FROM.
type CopySpec struct {
	Query string // SELECT query to run against the source
	Table string // destination table to COPY into
}

// DSN builds a postgres connection string from the pool config.
func DSN(pool *pgxpool.Pool) string {
	cfg := pool.Config().ConnConfig
	return cfg.ConnString()
}

// CopySchema dumps the schema from src and restores it into dst using
// pg_dump --schema-only and pg_restore.
func CopySchema(ctx context.Context, srcDSN, dstDSN string) error {
	dump := exec.CommandContext(ctx, "pg_dump",
		"--schema-only",
		"--format=custom",
		"--no-owner",
		"--no-privileges",
		srcDSN,
	)

	restore := exec.CommandContext(ctx, "pg_restore",
		"--no-owner",
		"--no-privileges",
		"-d", dstDSN,
	)

	pr, pw := io.Pipe()
	dump.Stdout = pw
	restore.Stdin = pr

	// Capture stderr for diagnostics.
	var dumpStderr, restoreStderr []byte
	dump.Stderr = &errWriter{buf: &dumpStderr}
	restore.Stderr = &errWriter{buf: &restoreStderr}

	if err := dump.Start(); err != nil {
		return fmt.Errorf("starting pg_dump: %w", err)
	}
	if err := restore.Start(); err != nil {
		return fmt.Errorf("starting pg_restore: %w", err)
	}

	// Close the write end once dump finishes so restore sees EOF.
	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- dump.Wait()
		_ = pw.Close()
	}()

	restoreErr := restore.Wait()
	dumpErr := <-dumpDone

	if dumpErr != nil {
		return fmt.Errorf("pg_dump failed: %w\nstderr: %s", dumpErr, dumpStderr)
	}
	if restoreErr != nil {
		return fmt.Errorf("pg_restore failed: %w\nstderr: %s", restoreErr, restoreStderr)
	}

	return nil
}

// CopyData copies rows from src to dst for each CopySpec.
// Each spec's Query is run as COPY (query) TO STDOUT on the source,
// and piped into COPY table FROM STDIN on the destination.
func CopyData(ctx context.Context, srcDSN, dstDSN string, specs []CopySpec) error {
	for _, spec := range specs {
		if err := copyTable(ctx, srcDSN, dstDSN, spec); err != nil {
			return fmt.Errorf("copying %s: %w", spec.Table, err)
		}
	}
	return nil
}

func copyTable(ctx context.Context, srcDSN, dstDSN string, spec CopySpec) error {
	copyOut := fmt.Sprintf(`COPY (%s) TO STDOUT`, spec.Query)
	copyIn := fmt.Sprintf(`COPY %s FROM STDIN`, spec.Table)

	src := exec.CommandContext(ctx, "psql", srcDSN, "-c", copyOut)
	dst := exec.CommandContext(ctx, "psql", dstDSN, "-c", copyIn)

	pr, pw := io.Pipe()
	src.Stdout = pw
	dst.Stdin = pr

	var srcStderr, dstStderr []byte
	src.Stderr = &errWriter{buf: &srcStderr}
	dst.Stderr = &errWriter{buf: &dstStderr}

	if err := src.Start(); err != nil {
		return fmt.Errorf("starting COPY TO: %w", err)
	}
	if err := dst.Start(); err != nil {
		return fmt.Errorf("starting COPY FROM: %w", err)
	}

	srcDone := make(chan error, 1)
	go func() {
		srcDone <- src.Wait()
		_ = pw.Close()
	}()

	dstErr := dst.Wait()
	srcErr := <-srcDone

	if srcErr != nil {
		return fmt.Errorf("COPY TO failed: %w\nstderr: %s", srcErr, srcStderr)
	}
	if dstErr != nil {
		return fmt.Errorf("COPY FROM failed: %w\nstderr: %s", dstErr, dstStderr)
	}

	return nil
}

// Branch performs a full branch operation: copies schema then selectively copies data.
func Branch(ctx context.Context, srcDSN, dstDSN string, specs []CopySpec) error {
	if err := CopySchema(ctx, srcDSN, dstDSN); err != nil {
		return fmt.Errorf("schema copy: %w", err)
	}
	if len(specs) > 0 {
		if err := CopyData(ctx, srcDSN, dstDSN, specs); err != nil {
			return fmt.Errorf("data copy: %w", err)
		}
	}
	return nil
}

// errWriter is a simple io.Writer that appends to a byte slice.
type errWriter struct {
	buf *[]byte
}

func (w *errWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

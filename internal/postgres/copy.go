package postgres

import (
	"context"
	"fmt"
	"io"
	"os/exec"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// CopyCallback is called after each table is successfully copied.
type CopyCallback func(spec CopySpec)

// CopyData copies rows from src to dst for each CopySpec within a single
// REPEATABLE READ transaction on the source, giving a point-in-time snapshot
// across all tables.
func CopyData(ctx context.Context, srcDSN, dstDSN string, specs []CopySpec) error {
	return CopyDataWithCallback(ctx, srcDSN, dstDSN, specs, nil)
}

// CopyDataWithCallback is like CopyData but calls cb after each table completes.
func CopyDataWithCallback(ctx context.Context, srcDSN, dstDSN string, specs []CopySpec, cb CopyCallback) error {
	srcConn, err := pgx.Connect(ctx, srcDSN)
	if err != nil {
		return fmt.Errorf("connecting to source: %w", err)
	}
	defer srcConn.Close(ctx) //nolint:errcheck

	tx, err := srcConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, spec := range specs {
		if err := copyTablePgx(ctx, tx.Conn().PgConn(), dstDSN, spec); err != nil {
			return fmt.Errorf("copying %s: %w", spec.Table, err)
		}
		if cb != nil {
			cb(spec)
		}
	}

	return tx.Commit(ctx)
}

// copyTablePgx copies a single table using pgx's native COPY protocol.
// srcPgConn must be inside the caller's transaction.
func copyTablePgx(ctx context.Context, srcPgConn *pgconn.PgConn, dstDSN string, spec CopySpec) error {
	dstConn, err := pgx.Connect(ctx, dstDSN)
	if err != nil {
		return fmt.Errorf("connecting to destination: %w", err)
	}
	defer dstConn.Close(ctx) //nolint:errcheck

	copyOutSQL := fmt.Sprintf(`COPY (%s) TO STDOUT`, spec.Query)
	copyInSQL := fmt.Sprintf(`COPY %s FROM STDIN`, spec.Table)

	pr, pw := io.Pipe()

	srcDone := make(chan error, 1)
	go func() {
		_, err := srcPgConn.CopyTo(ctx, pw, copyOutSQL)
		pw.CloseWithError(err) // signal EOF (or error) to the reader
		srcDone <- err
	}()

	_, dstErr := dstConn.PgConn().CopyFrom(ctx, pr, copyInSQL)
	srcErr := <-srcDone

	if srcErr != nil {
		return fmt.Errorf("COPY TO failed: %w", srcErr)
	}
	if dstErr != nil {
		return fmt.Errorf("COPY FROM failed: %w", dstErr)
	}

	return nil
}

// TableNames extracts the table names from a slice of CopySpecs.
func TableNames(specs []CopySpec) []string {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Table
	}
	return names
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

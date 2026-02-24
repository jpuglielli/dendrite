package tracker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WriteMeta creates the _dendrite_meta table in the sandbox and stores the
// source DSN, tracked table list, and branch timestamp.
func WriteMeta(ctx context.Context, pool *pgxpool.Pool, srcDSN string, tables []string) error {
	tablesJSON, err := json.Marshal(tables)
	if err != nil {
		return fmt.Errorf("marshalling tables: %w", err)
	}

	const ddl = `
		CREATE TABLE IF NOT EXISTS _dendrite_meta (
			key   TEXT PRIMARY KEY,
			value JSONB NOT NULL
		)`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("creating _dendrite_meta: %w", err)
	}

	const upsert = `
		INSERT INTO _dendrite_meta (key, value)
		VALUES ('source_dsn', to_jsonb($1::text)),
		       ('tables',     $2::jsonb),
		       ('branched_at', to_jsonb(now()::text))
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`
	if _, err := pool.Exec(ctx, upsert, srcDSN, tablesJSON); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}

	return nil
}

// ReadMeta reads the source DSN and tracked tables from _dendrite_meta.
// Returns a clear error if the sandbox predates tracking.
func ReadMeta(ctx context.Context, pool *pgxpool.Pool) (srcDSN string, tables []string, err error) {
	// Check if the meta table exists.
	var exists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = '_dendrite_meta'
		)`).Scan(&exists)
	if err != nil {
		return "", nil, fmt.Errorf("checking _dendrite_meta existence: %w", err)
	}
	if !exists {
		return "", nil, fmt.Errorf("sandbox predates change tracking (no _dendrite_meta table)")
	}

	// Read source DSN.
	var dsnJSON string
	err = pool.QueryRow(ctx, `SELECT value #>> '{}' FROM _dendrite_meta WHERE key = 'source_dsn'`).Scan(&dsnJSON)
	if err != nil {
		return "", nil, fmt.Errorf("reading source_dsn: %w", err)
	}
	srcDSN = dsnJSON

	// Read tables list.
	var tablesJSON []byte
	err = pool.QueryRow(ctx, `SELECT value FROM _dendrite_meta WHERE key = 'tables'`).Scan(&tablesJSON)
	if err != nil {
		return "", nil, fmt.Errorf("reading tables: %w", err)
	}
	if err := json.Unmarshal(tablesJSON, &tables); err != nil {
		return "", nil, fmt.Errorf("unmarshalling tables: %w", err)
	}

	return srcDSN, tables, nil
}

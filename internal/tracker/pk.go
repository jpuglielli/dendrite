package tracker

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DiscoverPKs returns the primary-key columns for each table, ordered by
// their position in the constraint. It queries pg_constraint + pg_attribute
// and errors if any table lacks a primary key.
func DiscoverPKs(ctx context.Context, pool *pgxpool.Pool, tables []string) (map[string][]string, error) {
	const q = `
		SELECT a.attname
		FROM   pg_constraint c
		JOIN   pg_attribute  a ON a.attrelid = c.conrelid
		                      AND a.attnum = ANY(c.conkey)
		WHERE  c.contype   = 'p'
		  AND  c.conrelid  = $1::regclass
		ORDER BY array_position(c.conkey, a.attnum)`

	pks := make(map[string][]string, len(tables))
	for _, tbl := range tables {
		rows, err := pool.Query(ctx, q, tbl)
		if err != nil {
			return nil, fmt.Errorf("querying PK for %s: %w", tbl, err)
		}

		var cols []string
		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scanning PK column for %s: %w", tbl, err)
			}
			cols = append(cols, col)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating PK rows for %s: %w", tbl, err)
		}

		if len(cols) == 0 {
			return nil, fmt.Errorf("table %s has no primary key", tbl)
		}
		pks[tbl] = cols
	}
	return pks, nil
}

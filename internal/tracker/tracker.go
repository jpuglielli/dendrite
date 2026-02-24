package tracker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ChangeOp represents a DML operation type.
type ChangeOp string

const (
	OpInsert ChangeOp = "INSERT"
	OpUpdate ChangeOp = "UPDATE"
	OpDelete ChangeOp = "DELETE"
)

// Change is the net effect of one or more DML operations on a single row.
type Change struct {
	Table  string
	Op     ChangeOp
	PKCols []string
	PKVals map[string]any
	OldRow map[string]any // nil for INSERT
	NewRow map[string]any // nil for DELETE
}

// Install creates the _dendrite_changes table, the trigger function, and
// attaches AFTER INSERT/UPDATE/DELETE triggers to each listed table.
// The pool must point at the sandbox database.
func Install(ctx context.Context, pool *pgxpool.Pool, tables []string) error {
	const changesTable = `
		CREATE TABLE IF NOT EXISTS _dendrite_changes (
			seq        BIGSERIAL PRIMARY KEY,
			table_name TEXT    NOT NULL,
			op         TEXT    NOT NULL,
			pk_cols    JSONB   NOT NULL,
			pk_vals    JSONB   NOT NULL,
			old_row    JSONB,
			new_row    JSONB,
			ts         TIMESTAMPTZ NOT NULL DEFAULT now()
		)`
	if _, err := pool.Exec(ctx, changesTable); err != nil {
		return fmt.Errorf("creating _dendrite_changes: %w", err)
	}

	// PL/pgSQL trigger function that looks up PK columns from pg_constraint
	// and records row images as JSONB. Uses to_jsonb(OLD/NEW) and filters
	// to PK columns via jsonb_each + jsonb_object_agg.
	const triggerFunc = `
		CREATE OR REPLACE FUNCTION _dendrite_track_changes() RETURNS trigger AS $$
		DECLARE
			pk_cols jsonb;
			pk_vals jsonb;
			ref     record;
		BEGIN
			-- Look up primary-key columns for the firing table.
			SELECT jsonb_agg(a.attname ORDER BY array_position(c.conkey, a.attnum))
			INTO   pk_cols
			FROM   pg_constraint c
			JOIN   pg_attribute  a ON a.attrelid = c.conrelid
			                      AND a.attnum = ANY(c.conkey)
			WHERE  c.contype  = 'p'
			  AND  c.conrelid = TG_RELID;

			IF pk_cols IS NULL THEN
				RAISE EXCEPTION 'table % has no primary key — cannot track changes', TG_TABLE_NAME;
			END IF;

			-- Extract PK values from the appropriate row record.
			-- For DELETE use OLD, otherwise use NEW.
			IF TG_OP = 'DELETE' THEN
				pk_vals := (
					SELECT jsonb_object_agg(kv.key, kv.value)
					FROM   jsonb_each(to_jsonb(OLD)) AS kv
					WHERE  kv.key IN (SELECT jsonb_array_elements_text(pk_cols))
				);
			ELSE
				pk_vals := (
					SELECT jsonb_object_agg(kv.key, kv.value)
					FROM   jsonb_each(to_jsonb(NEW)) AS kv
					WHERE  kv.key IN (SELECT jsonb_array_elements_text(pk_cols))
				);
			END IF;

			INSERT INTO _dendrite_changes (table_name, op, pk_cols, pk_vals, old_row, new_row)
			VALUES (
				TG_TABLE_NAME,
				TG_OP,
				pk_cols,
				pk_vals,
				CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) END,
				CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) END
			);

			RETURN COALESCE(NEW, OLD);
		END;
		$$ LANGUAGE plpgsql`

	if _, err := pool.Exec(ctx, triggerFunc); err != nil {
		return fmt.Errorf("creating trigger function: %w", err)
	}

	// Attach triggers to each table.
	for _, tbl := range tables {
		stmt := fmt.Sprintf(`
			CREATE OR REPLACE TRIGGER _dendrite_track
			AFTER INSERT OR UPDATE OR DELETE ON %q
			FOR EACH ROW EXECUTE FUNCTION _dendrite_track_changes()`, tbl)
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("creating trigger on %s: %w", tbl, err)
		}
	}

	return nil
}

// changeRow is the raw row from _dendrite_changes.
type changeRow struct {
	Seq    int64
	Table  string
	Op     ChangeOp
	PKCols []string
	PKVals map[string]any
	OldRow map[string]any
	NewRow map[string]any
}

// changeAccum accumulates operations on a single (table, pk) for collapsing.
type changeAccum struct {
	first changeRow   // first operation (for original old_row)
	last  changeRow   // last operation (for final new_row/op)
	ops   []ChangeOp
}

// ReadChanges reads all recorded changes from _dendrite_changes, then
// collapses multiple operations on the same (table, pk) into a net effect.
func ReadChanges(ctx context.Context, pool *pgxpool.Pool) ([]Change, error) {
	// Check if the changes table exists.
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = '_dendrite_changes'
		)`).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("checking _dendrite_changes existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("sandbox predates change tracking (no _dendrite_changes table)")
	}

	rows, err := pool.Query(ctx, `
		SELECT seq, table_name, op, pk_cols, pk_vals, old_row, new_row
		FROM   _dendrite_changes
		ORDER BY seq`)
	if err != nil {
		return nil, fmt.Errorf("querying changes: %w", err)
	}
	defer rows.Close()

	// Group changes by (table, pk) for collapsing.
	type rowKey struct {
		table  string
		pkJSON string // canonical JSON of pk_vals for grouping
	}

	seen := make(map[rowKey]*changeAccum)
	var order []rowKey // preserve insertion order

	for rows.Next() {
		var cr changeRow
		var pkColsJSON, pkValsJSON, oldRowJSON, newRowJSON []byte

		if err := rows.Scan(&cr.Seq, &cr.Table, &cr.Op, &pkColsJSON, &pkValsJSON, &oldRowJSON, &newRowJSON); err != nil {
			return nil, fmt.Errorf("scanning change row: %w", err)
		}
		if err := json.Unmarshal(pkColsJSON, &cr.PKCols); err != nil {
			return nil, fmt.Errorf("unmarshalling pk_cols: %w", err)
		}
		if err := json.Unmarshal(pkValsJSON, &cr.PKVals); err != nil {
			return nil, fmt.Errorf("unmarshalling pk_vals: %w", err)
		}
		if oldRowJSON != nil {
			if err := json.Unmarshal(oldRowJSON, &cr.OldRow); err != nil {
				return nil, fmt.Errorf("unmarshalling old_row: %w", err)
			}
		}
		if newRowJSON != nil {
			if err := json.Unmarshal(newRowJSON, &cr.NewRow); err != nil {
				return nil, fmt.Errorf("unmarshalling new_row: %w", err)
			}
		}

		key := rowKey{table: cr.Table, pkJSON: string(pkValsJSON)}
		if a, ok := seen[key]; ok {
			a.last = cr
			a.ops = append(a.ops, cr.Op)
		} else {
			a := &changeAccum{first: cr, last: cr, ops: []ChangeOp{cr.Op}}
			seen[key] = a
			order = append(order, key)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating change rows: %w", err)
	}

	// Collapse each (table, pk) into a net effect.
	var changes []Change
	for _, key := range order {
		a := seen[key]
		c := collapse(a)
		if c != nil {
			changes = append(changes, *c)
		}
	}
	return changes, nil
}

// collapse reduces a sequence of operations on a single row to its net effect.
func collapse(a *changeAccum) *Change {
	firstOp := a.ops[0]
	lastOp := a.ops[len(a.ops)-1]

	switch {
	case firstOp == OpInsert && lastOp == OpDelete:
		// INSERT → ... → DELETE = no-op
		return nil

	case firstOp == OpInsert:
		// INSERT → ... → (INSERT or UPDATE) = net INSERT with final row
		return &Change{
			Table:  a.first.Table,
			Op:     OpInsert,
			PKCols: a.last.PKCols,
			PKVals: a.last.PKVals,
			NewRow: a.last.NewRow,
		}

	case firstOp == OpUpdate && lastOp == OpDelete:
		// UPDATE → ... → DELETE = net DELETE with original old_row
		return &Change{
			Table:  a.first.Table,
			Op:     OpDelete,
			PKCols: a.first.PKCols,
			PKVals: a.first.PKVals,
			OldRow: a.first.OldRow,
		}

	case firstOp == OpUpdate:
		// UPDATE → ... → UPDATE = net UPDATE (first old_row, last new_row)
		return &Change{
			Table:  a.first.Table,
			Op:     OpUpdate,
			PKCols: a.last.PKCols,
			PKVals: a.last.PKVals,
			OldRow: a.first.OldRow,
			NewRow: a.last.NewRow,
		}

	case firstOp == OpDelete && lastOp == OpInsert:
		// DELETE → INSERT = net UPDATE (old_row from delete, new_row from insert)
		return &Change{
			Table:  a.first.Table,
			Op:     OpUpdate,
			PKCols: a.last.PKCols,
			PKVals: a.last.PKVals,
			OldRow: a.first.OldRow,
			NewRow: a.last.NewRow,
		}

	case firstOp == OpDelete && lastOp == OpDelete:
		// DELETE (only or repeated) = net DELETE
		return &Change{
			Table:  a.first.Table,
			Op:     OpDelete,
			PKCols: a.first.PKCols,
			PKVals: a.first.PKVals,
			OldRow: a.first.OldRow,
		}

	case firstOp == OpDelete:
		// DELETE → ... → UPDATE = UPDATE (old from original delete, new from last)
		return &Change{
			Table:  a.first.Table,
			Op:     OpUpdate,
			PKCols: a.last.PKCols,
			PKVals: a.last.PKVals,
			OldRow: a.first.OldRow,
			NewRow: a.last.NewRow,
		}

	default:
		// Single operation, return as-is.
		return &Change{
			Table:  a.first.Table,
			Op:     a.first.Op,
			PKCols: a.first.PKCols,
			PKVals: a.first.PKVals,
			OldRow: a.first.OldRow,
			NewRow: a.first.NewRow,
		}
	}
}

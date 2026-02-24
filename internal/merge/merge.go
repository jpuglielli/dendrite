package merge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpuglielli/dendrite/internal/tracker"
)

// DiffResult holds the outcome of comparing sandbox changes against the source.
type DiffResult struct {
	Tables    []TableDiff
	Conflicts []Conflict
}

// TableDiff holds the net changes for a single table.
type TableDiff struct {
	Table   string
	PKCols  []string
	Inserts []map[string]any
	Updates []UpdateRow
	Deletes []map[string]any // old rows (pk values used for WHERE)
}

// UpdateRow pairs the base row with the desired new row.
type UpdateRow struct {
	OldRow map[string]any
	NewRow map[string]any
}

// Conflict describes a merge conflict for a single row.
type Conflict struct {
	Table      string
	PKVals     map[string]any
	Op         string
	Reason     string
	BaseRow    map[string]any // what was originally branched
	SourceRow  map[string]any // current source state
	SandboxRow map[string]any // sandbox state
}

// MergeOpts controls merge behavior.
type MergeOpts struct {
	DryRun     bool
	Force      bool // skip conflicted rows instead of aborting
	MaxRetries int  // default 3
}

// Diff compares sandbox changes against the current source state and returns
// clean changes plus any conflicts.
func Diff(ctx context.Context, srcPool, sandboxPool *pgxpool.Pool, tables []string) (*DiffResult, error) {
	changes, err := tracker.ReadChanges(ctx, sandboxPool)
	if err != nil {
		return nil, fmt.Errorf("reading sandbox changes: %w", err)
	}

	if len(changes) == 0 {
		return &DiffResult{}, nil
	}

	// Open a REPEATABLE READ transaction on source for consistent reads.
	srcTx, err := srcPool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return nil, fmt.Errorf("starting source transaction: %w", err)
	}
	defer func() { _ = srcTx.Rollback(ctx) }()

	// Build table index for ordering.
	tableIdx := make(map[string]int, len(tables))
	for i, t := range tables {
		tableIdx[t] = i
	}

	// Accumulate diffs per table.
	diffs := make(map[string]*TableDiff)
	var conflicts []Conflict

	for _, ch := range changes {
		td, ok := diffs[ch.Table]
		if !ok {
			td = &TableDiff{Table: ch.Table, PKCols: ch.PKCols}
			diffs[ch.Table] = td
		}

		// Fetch current source row by PK.
		srcRow, err := fetchRow(ctx, srcTx, ch.Table, ch.PKCols, ch.PKVals)
		if err != nil {
			return nil, fmt.Errorf("fetching source row %s %v: %w", ch.Table, ch.PKVals, err)
		}

		switch ch.Op {
		case tracker.OpInsert:
			if srcRow != nil {
				// PK already exists in source — conflict.
				conflicts = append(conflicts, Conflict{
					Table:      ch.Table,
					PKVals:     ch.PKVals,
					Op:         string(ch.Op),
					Reason:     "row with same PK already exists in source",
					SourceRow:  srcRow,
					SandboxRow: ch.NewRow,
				})
			} else {
				td.Inserts = append(td.Inserts, ch.NewRow)
			}

		case tracker.OpUpdate:
			if srcRow == nil {
				// Source row was deleted since branching — conflict.
				conflicts = append(conflicts, Conflict{
					Table:      ch.Table,
					PKVals:     ch.PKVals,
					Op:         string(ch.Op),
					Reason:     "row was deleted from source since branching",
					BaseRow:    ch.OldRow,
					SandboxRow: ch.NewRow,
				})
			} else if !rowsEqual(ch.OldRow, srcRow) {
				// Source row changed since branching — conflict.
				conflicts = append(conflicts, Conflict{
					Table:      ch.Table,
					PKVals:     ch.PKVals,
					Op:         string(ch.Op),
					Reason:     "source row changed since branching",
					BaseRow:    ch.OldRow,
					SourceRow:  srcRow,
					SandboxRow: ch.NewRow,
				})
			} else {
				td.Updates = append(td.Updates, UpdateRow{
					OldRow: ch.OldRow,
					NewRow: ch.NewRow,
				})
			}

		case tracker.OpDelete:
			if srcRow == nil {
				// Already gone — no-op, skip silently.
				continue
			}
			if !rowsEqual(ch.OldRow, srcRow) {
				// Source row changed since branching — conflict.
				conflicts = append(conflicts, Conflict{
					Table:      ch.Table,
					PKVals:     ch.PKVals,
					Op:         string(ch.Op),
					Reason:     "source row changed since branching",
					BaseRow:    ch.OldRow,
					SourceRow:  srcRow,
				})
			} else {
				td.Deletes = append(td.Deletes, ch.OldRow)
			}
		}
	}

	if err := srcTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing source read transaction: %w", err)
	}

	// Order table diffs by FK order.
	ordered := make([]TableDiff, 0, len(diffs))
	for _, t := range tables {
		if td, ok := diffs[t]; ok {
			ordered = append(ordered, *td)
		}
	}
	// Include any tables not in the provided order (shouldn't happen, but safe).
	for tbl, td := range diffs {
		if _, ok := tableIdx[tbl]; !ok {
			ordered = append(ordered, *td)
		}
	}

	return &DiffResult{Tables: ordered, Conflicts: conflicts}, nil
}

// Merge applies the diff to the source database.
func Merge(ctx context.Context, srcPool *pgxpool.Pool, diff *DiffResult, tables []string, opts MergeOpts) error {
	if len(diff.Conflicts) > 0 && !opts.Force {
		return fmt.Errorf("%d conflict(s) detected — use --force to skip conflicted rows", len(diff.Conflicts))
	}

	if opts.DryRun {
		return nil // caller should use GenerateSQL separately
	}

	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := applyDiff(ctx, srcPool, diff, tables)
		if err == nil {
			return nil
		}

		// Check for serialization failure (40001) — retry.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40001" {
			if attempt < maxRetries {
				continue
			}
			return fmt.Errorf("serialization failure after %d retries: %w", maxRetries, err)
		}
		return err
	}
	return nil
}

func applyDiff(ctx context.Context, srcPool *pgxpool.Pool, diff *DiffResult, tables []string) error {
	tx, err := srcPool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("starting merge transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Build a lookup from table name to TableDiff.
	diffByTable := make(map[string]*TableDiff, len(diff.Tables))
	for i := range diff.Tables {
		diffByTable[diff.Tables[i].Table] = &diff.Tables[i]
	}

	// Process DELETEs in reverse FK order (child tables first).
	for i := len(tables) - 1; i >= 0; i-- {
		td, ok := diffByTable[tables[i]]
		if !ok {
			continue
		}
		for _, oldRow := range td.Deletes {
			if err := execDelete(ctx, tx, td.Table, td.PKCols, oldRow); err != nil {
				return err
			}
		}
	}

	// Process INSERTs in FK order.
	for _, tbl := range tables {
		td, ok := diffByTable[tbl]
		if !ok {
			continue
		}
		for _, newRow := range td.Inserts {
			if err := execInsert(ctx, tx, td.Table, newRow); err != nil {
				return err
			}
		}
	}

	// Process UPDATEs in FK order.
	for _, tbl := range tables {
		td, ok := diffByTable[tbl]
		if !ok {
			continue
		}
		for _, upd := range td.Updates {
			if err := execUpdate(ctx, tx, td.Table, td.PKCols, upd); err != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

// GenerateSQL returns human-readable SQL statements for a dry-run preview.
func GenerateSQL(diff *DiffResult) []string {
	var stmts []string
	for _, td := range diff.Tables {
		for _, oldRow := range td.Deletes {
			stmts = append(stmts, buildDeleteSQL(td.Table, td.PKCols, oldRow))
		}
		for _, newRow := range td.Inserts {
			stmts = append(stmts, buildInsertSQL(td.Table, newRow))
		}
		for _, upd := range td.Updates {
			stmts = append(stmts, buildUpdateSQL(td.Table, td.PKCols, upd))
		}
	}
	return stmts
}

// --- SQL execution helpers ---

func fetchRow(ctx context.Context, tx pgx.Tx, table string, pkCols []string, pkVals map[string]any) (map[string]any, error) {
	whereParts := make([]string, len(pkCols))
	args := make([]any, len(pkCols))
	for i, col := range pkCols {
		whereParts[i] = fmt.Sprintf("%q = $%d", col, i+1)
		args[i] = pkVals[col]
	}

	q := fmt.Sprintf(`SELECT to_jsonb(t) FROM %q t WHERE %s`, table, strings.Join(whereParts, " AND "))

	var rowJSON []byte
	err := tx.QueryRow(ctx, q, args...).Scan(&rowJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var row map[string]any
	if err := json.Unmarshal(rowJSON, &row); err != nil {
		return nil, fmt.Errorf("unmarshalling source row: %w", err)
	}
	return row, nil
}

func execDelete(ctx context.Context, tx pgx.Tx, table string, pkCols []string, oldRow map[string]any) error {
	whereParts := make([]string, len(pkCols))
	args := make([]any, len(pkCols))
	for i, col := range pkCols {
		whereParts[i] = fmt.Sprintf("%q = $%d", col, i+1)
		args[i] = oldRow[col]
	}

	q := fmt.Sprintf(`DELETE FROM %q WHERE %s`, table, strings.Join(whereParts, " AND "))
	_, err := tx.Exec(ctx, q, args...)
	return err
}

func execInsert(ctx context.Context, tx pgx.Tx, table string, newRow map[string]any) error {
	cols := make([]string, 0, len(newRow))
	placeholders := make([]string, 0, len(newRow))
	args := make([]any, 0, len(newRow))
	i := 1
	for col, val := range newRow {
		cols = append(cols, fmt.Sprintf("%q", col))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		args = append(args, val)
		i++
	}

	q := fmt.Sprintf(`INSERT INTO %q (%s) VALUES (%s)`,
		table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
	_, err := tx.Exec(ctx, q, args...)
	return err
}

func execUpdate(ctx context.Context, tx pgx.Tx, table string, pkCols []string, upd UpdateRow) error {
	// SET clauses for changed columns.
	setClauses := make([]string, 0)
	args := make([]any, 0)
	idx := 1
	for col, val := range upd.NewRow {
		setClauses = append(setClauses, fmt.Sprintf("%q = $%d", col, idx))
		args = append(args, val)
		idx++
	}

	// WHERE on PK columns.
	whereParts := make([]string, len(pkCols))
	for i, col := range pkCols {
		whereParts[i] = fmt.Sprintf("%q = $%d", col, idx)
		args = append(args, upd.NewRow[col])
		idx++
	}

	q := fmt.Sprintf(`UPDATE %q SET %s WHERE %s`,
		table, strings.Join(setClauses, ", "), strings.Join(whereParts, " AND "))
	_, err := tx.Exec(ctx, q, args...)
	return err
}

// --- SQL generation helpers (for dry-run) ---

func buildDeleteSQL(table string, pkCols []string, oldRow map[string]any) string {
	whereParts := make([]string, len(pkCols))
	for i, col := range pkCols {
		whereParts[i] = fmt.Sprintf("%q = %s", col, sqlLiteral(oldRow[col]))
	}
	return fmt.Sprintf("DELETE FROM %q WHERE %s;", table, strings.Join(whereParts, " AND "))
}

func buildInsertSQL(table string, newRow map[string]any) string {
	cols := make([]string, 0, len(newRow))
	vals := make([]string, 0, len(newRow))
	for col, val := range newRow {
		cols = append(cols, fmt.Sprintf("%q", col))
		vals = append(vals, sqlLiteral(val))
	}
	return fmt.Sprintf("INSERT INTO %q (%s) VALUES (%s);",
		table, strings.Join(cols, ", "), strings.Join(vals, ", "))
}

func buildUpdateSQL(table string, pkCols []string, upd UpdateRow) string {
	setClauses := make([]string, 0)
	for col, val := range upd.NewRow {
		setClauses = append(setClauses, fmt.Sprintf("%q = %s", col, sqlLiteral(val)))
	}
	whereParts := make([]string, len(pkCols))
	for i, col := range pkCols {
		whereParts[i] = fmt.Sprintf("%q = %s", col, sqlLiteral(upd.NewRow[col]))
	}
	return fmt.Sprintf("UPDATE %q SET %s WHERE %s;",
		table, strings.Join(setClauses, ", "), strings.Join(whereParts, " AND "))
}

func sqlLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case float64:
		// JSON numbers are float64; render integers without decimal.
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("'%v'", val)
	}
}

// rowsEqual compares two JSONB row maps for equality.
// Both maps come from to_jsonb() so types should be consistent.
func rowsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if fmt.Sprintf("%v", av) != fmt.Sprintf("%v", bv) {
			return false
		}
	}
	return true
}

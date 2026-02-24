package merge

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpuglielli/dendrite/internal/tracker"
)

// --- Unit tests (no DB required) ---

func TestRowsEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b map[string]any
		want bool
	}{
		{"identical", m("id", 1, "name", "alice"), m("id", 1, "name", "alice"), true},
		{"different value", m("id", 1, "name", "alice"), m("id", 1, "name", "bob"), false},
		{"missing key", m("id", 1, "name", "alice"), m("id", 1), false},
		{"extra key", m("id", 1), m("id", 1, "name", "alice"), false},
		{"both nil", nil, nil, true},
		{"one nil", m("id", 1), nil, false},
		{"empty maps", map[string]any{}, map[string]any{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rowsEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("rowsEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestSqlLiteral(t *testing.T) {
	tests := []struct {
		input any
		want  string
	}{
		{nil, "NULL"},
		{float64(42), "42"},
		{float64(3.14), "3.14"},
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{true, "TRUE"},
		{false, "FALSE"},
	}
	for _, tt := range tests {
		got := sqlLiteral(tt.input)
		if got != tt.want {
			t.Errorf("sqlLiteral(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestGenerateSQL_Insert(t *testing.T) {
	diff := &DiffResult{
		Tables: []TableDiff{
			{
				Table:   "items",
				PKCols:  []string{"id"},
				Inserts: []map[string]any{{"id": float64(1), "name": "widget"}},
			},
		},
	}
	stmts := GenerateSQL(diff)
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	// Should contain INSERT INTO.
	if got := stmts[0]; !contains(got, "INSERT INTO") {
		t.Errorf("expected INSERT statement, got %q", got)
	}
}

func TestGenerateSQL_Update(t *testing.T) {
	diff := &DiffResult{
		Tables: []TableDiff{
			{
				Table:  "items",
				PKCols: []string{"id"},
				Updates: []UpdateRow{
					{
						OldRow: map[string]any{"id": float64(1), "name": "widget"},
						NewRow: map[string]any{"id": float64(1), "name": "gadget"},
					},
				},
			},
		},
	}
	stmts := GenerateSQL(diff)
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	if got := stmts[0]; !contains(got, "UPDATE") {
		t.Errorf("expected UPDATE statement, got %q", got)
	}
}

func TestGenerateSQL_Delete(t *testing.T) {
	diff := &DiffResult{
		Tables: []TableDiff{
			{
				Table:   "items",
				PKCols:  []string{"id"},
				Deletes: []map[string]any{{"id": float64(1), "name": "widget"}},
			},
		},
	}
	stmts := GenerateSQL(diff)
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	if got := stmts[0]; !contains(got, "DELETE FROM") {
		t.Errorf("expected DELETE statement, got %q", got)
	}
}

func TestGenerateSQL_Empty(t *testing.T) {
	diff := &DiffResult{}
	stmts := GenerateSQL(diff)
	if len(stmts) != 0 {
		t.Fatalf("expected 0 statements, got %d", len(stmts))
	}
}

func TestMerge_RejectsConflictsWithoutForce(t *testing.T) {
	diff := &DiffResult{
		Conflicts: []Conflict{{Table: "items", Op: "UPDATE", Reason: "source changed"}},
	}
	err := Merge(context.Background(), nil, diff, nil, MergeOpts{})
	if err == nil {
		t.Fatal("expected error for conflicts without --force")
	}
}

func TestMerge_DryRunReturnsNil(t *testing.T) {
	diff := &DiffResult{
		Tables: []TableDiff{{Table: "items", PKCols: []string{"id"}, Inserts: []map[string]any{{"id": float64(1)}}}},
	}
	err := Merge(context.Background(), nil, diff, []string{"items"}, MergeOpts{DryRun: true})
	if err != nil {
		t.Fatalf("expected nil for dry-run, got %v", err)
	}
}

// --- Integration tests (require running compose Postgres) ---

func adminDSN() string {
	user := envOr("DST_USER", "devuser")
	pass := envOr("DST_PASSWORD", "devpass")
	host := envOr("DST_HOST", "localhost")
	port := envOr("DST_PORT", "15432")
	return fmt.Sprintf("postgres://%s:%s@%s:%s/postgres", user, pass, host, port)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func skipIfNoPostgres(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, adminDSN())
	if err != nil {
		t.Skipf("skipping: cannot connect to Postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping: cannot ping Postgres: %v", err)
	}
	pool.Close()
}

// testDBPair creates two temporary databases (source + sandbox) with identical
// schema and seed data, installs tracking on the sandbox, and returns pools to both.
type testDBPair struct {
	srcPool     *pgxpool.Pool
	sandboxPool *pgxpool.Pool
	tables      []string
	cleanup     func()
}

func createTestDBPair(t *testing.T, ctx context.Context) *testDBPair {
	t.Helper()

	adminPool, err := pgxpool.New(ctx, adminDSN())
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}

	suffix := time.Now().UnixNano()
	srcName := fmt.Sprintf("test_merge_src_%d", suffix)
	sbName := fmt.Sprintf("test_merge_sb_%d", suffix)

	for _, name := range []string{srcName, sbName} {
		_, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, name))
		if err != nil {
			adminPool.Close()
			t.Fatalf("creating %s: %v", name, err)
		}
	}

	user := envOr("DST_USER", "devuser")
	pass := envOr("DST_PASSWORD", "devpass")
	host := envOr("DST_HOST", "localhost")
	port := envOr("DST_PORT", "15432")

	srcDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, srcName)
	sbDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, sbName)

	srcPool, err := pgxpool.New(ctx, srcDSN)
	if err != nil {
		adminPool.Close()
		t.Fatalf("connecting to source: %v", err)
	}

	sandboxPool, err := pgxpool.New(ctx, sbDSN)
	if err != nil {
		srcPool.Close()
		adminPool.Close()
		t.Fatalf("connecting to sandbox: %v", err)
	}

	// Create identical schema in both.
	schema := `
		CREATE TABLE items (
			id   INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			qty  INTEGER NOT NULL DEFAULT 0
		)`
	for _, p := range []*pgxpool.Pool{srcPool, sandboxPool} {
		if _, err := p.Exec(ctx, schema); err != nil {
			t.Fatalf("creating schema: %v", err)
		}
	}

	// Seed identical data in both.
	seed := `INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10), (2, 'gadget', 20), (3, 'doohickey', 30)`
	for _, p := range []*pgxpool.Pool{srcPool, sandboxPool} {
		if _, err := p.Exec(ctx, seed); err != nil {
			t.Fatalf("seeding data: %v", err)
		}
	}

	// Install tracking on sandbox.
	tables := []string{"items"}
	if err := tracker.Install(ctx, sandboxPool, tables); err != nil {
		t.Fatalf("installing tracker: %v", err)
	}

	cleanup := func() {
		srcPool.Close()
		sandboxPool.Close()
		bgCtx := context.Background()
		_, _ = adminPool.Exec(bgCtx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, srcName))
		_, _ = adminPool.Exec(bgCtx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, sbName))
		adminPool.Close()
	}

	return &testDBPair{
		srcPool:     srcPool,
		sandboxPool: sandboxPool,
		tables:      tables,
		cleanup:     cleanup,
	}
}

func TestDiff_NoChanges(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(diff.Conflicts))
	}
	for _, td := range diff.Tables {
		if len(td.Inserts)+len(td.Updates)+len(td.Deletes) != 0 {
			t.Errorf("expected no changes in %s", td.Table)
		}
	}
}

func TestDiff_InsertInSandbox(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Insert a new row in sandbox.
	_, err := pair.sandboxPool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (99, 'new_item', 5)`)
	if err != nil {
		t.Fatalf("sandbox insert: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(diff.Conflicts))
	}

	total := 0
	for _, td := range diff.Tables {
		total += len(td.Inserts)
	}
	if total != 1 {
		t.Errorf("expected 1 insert, got %d", total)
	}
}

func TestDiff_UpdateInSandbox_NoConflict(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Update a row in sandbox (source unchanged).
	_, err := pair.sandboxPool.Exec(ctx, `UPDATE items SET name = 'super_widget' WHERE id = 1`)
	if err != nil {
		t.Fatalf("sandbox update: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(diff.Conflicts))
	}

	total := 0
	for _, td := range diff.Tables {
		total += len(td.Updates)
	}
	if total != 1 {
		t.Errorf("expected 1 update, got %d", total)
	}
}

func TestDiff_DeleteInSandbox_NoConflict(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Delete a row in sandbox (source unchanged).
	_, err := pair.sandboxPool.Exec(ctx, `DELETE FROM items WHERE id = 3`)
	if err != nil {
		t.Fatalf("sandbox delete: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(diff.Conflicts))
	}

	total := 0
	for _, td := range diff.Tables {
		total += len(td.Deletes)
	}
	if total != 1 {
		t.Errorf("expected 1 delete, got %d", total)
	}
}

func TestDiff_UpdateConflict_SourceChanged(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Update in sandbox.
	_, err := pair.sandboxPool.Exec(ctx, `UPDATE items SET name = 'sandbox_name' WHERE id = 1`)
	if err != nil {
		t.Fatalf("sandbox update: %v", err)
	}

	// Also update in source — creates conflict.
	_, err = pair.srcPool.Exec(ctx, `UPDATE items SET name = 'source_name' WHERE id = 1`)
	if err != nil {
		t.Fatalf("source update: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(diff.Conflicts))
	}
	if diff.Conflicts[0].Op != "UPDATE" {
		t.Errorf("conflict Op = %q, want UPDATE", diff.Conflicts[0].Op)
	}
}

func TestDiff_InsertConflict_PKCollision(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Insert in sandbox with id=99.
	_, err := pair.sandboxPool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (99, 'sb_item', 1)`)
	if err != nil {
		t.Fatalf("sandbox insert: %v", err)
	}

	// Also insert in source with same PK.
	_, err = pair.srcPool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (99, 'src_item', 2)`)
	if err != nil {
		t.Fatalf("source insert: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(diff.Conflicts))
	}
	if diff.Conflicts[0].Op != "INSERT" {
		t.Errorf("conflict Op = %q, want INSERT", diff.Conflicts[0].Op)
	}
}

func TestDiff_DeleteNoOp_SourceAlreadyGone(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Delete in sandbox.
	_, err := pair.sandboxPool.Exec(ctx, `DELETE FROM items WHERE id = 3`)
	if err != nil {
		t.Fatalf("sandbox delete: %v", err)
	}

	// Also delete in source — should be a no-op, not a conflict.
	_, err = pair.srcPool.Exec(ctx, `DELETE FROM items WHERE id = 3`)
	if err != nil {
		t.Fatalf("source delete: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(diff.Conflicts))
	}
	// The delete should be silently skipped — no changes at all.
	total := 0
	for _, td := range diff.Tables {
		total += len(td.Inserts) + len(td.Updates) + len(td.Deletes)
	}
	if total != 0 {
		t.Errorf("expected 0 net changes (no-op delete), got %d", total)
	}
}

func TestMerge_Insert(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Insert in sandbox.
	_, err := pair.sandboxPool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (99, 'new_item', 5)`)
	if err != nil {
		t.Fatalf("sandbox insert: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	err = Merge(ctx, pair.srcPool, diff, pair.tables, MergeOpts{MaxRetries: 1})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// Verify source has the new row.
	var name string
	err = pair.srcPool.QueryRow(ctx, `SELECT name FROM items WHERE id = 99`).Scan(&name)
	if err != nil {
		t.Fatalf("querying merged row: %v", err)
	}
	if name != "new_item" {
		t.Errorf("name = %q, want new_item", name)
	}
}

func TestMerge_Update(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Update in sandbox.
	_, err := pair.sandboxPool.Exec(ctx, `UPDATE items SET name = 'super_widget' WHERE id = 1`)
	if err != nil {
		t.Fatalf("sandbox update: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	err = Merge(ctx, pair.srcPool, diff, pair.tables, MergeOpts{MaxRetries: 1})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	var name string
	err = pair.srcPool.QueryRow(ctx, `SELECT name FROM items WHERE id = 1`).Scan(&name)
	if err != nil {
		t.Fatalf("querying merged row: %v", err)
	}
	if name != "super_widget" {
		t.Errorf("name = %q, want super_widget", name)
	}
}

func TestMerge_Delete(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Delete in sandbox.
	_, err := pair.sandboxPool.Exec(ctx, `DELETE FROM items WHERE id = 3`)
	if err != nil {
		t.Fatalf("sandbox delete: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}

	err = Merge(ctx, pair.srcPool, diff, pair.tables, MergeOpts{MaxRetries: 1})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// Verify source no longer has the row.
	var count int
	err = pair.srcPool.QueryRow(ctx, `SELECT count(*) FROM items WHERE id = 3`).Scan(&count)
	if err != nil {
		t.Fatalf("querying deleted row: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows with id=3, got %d", count)
	}
}

func TestMerge_MixedOperations(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Insert, update, and delete in sandbox.
	_, err := pair.sandboxPool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (99, 'new_item', 5)`)
	if err != nil {
		t.Fatalf("sandbox insert: %v", err)
	}
	_, err = pair.sandboxPool.Exec(ctx, `UPDATE items SET qty = 999 WHERE id = 1`)
	if err != nil {
		t.Fatalf("sandbox update: %v", err)
	}
	_, err = pair.sandboxPool.Exec(ctx, `DELETE FROM items WHERE id = 3`)
	if err != nil {
		t.Fatalf("sandbox delete: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 0 {
		t.Fatalf("expected 0 conflicts, got %d", len(diff.Conflicts))
	}

	err = Merge(ctx, pair.srcPool, diff, pair.tables, MergeOpts{MaxRetries: 1})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// Verify all three changes applied.
	var name string
	err = pair.srcPool.QueryRow(ctx, `SELECT name FROM items WHERE id = 99`).Scan(&name)
	if err != nil {
		t.Fatalf("querying inserted row: %v", err)
	}
	if name != "new_item" {
		t.Errorf("inserted name = %q, want new_item", name)
	}

	var qty int
	err = pair.srcPool.QueryRow(ctx, `SELECT qty FROM items WHERE id = 1`).Scan(&qty)
	if err != nil {
		t.Fatalf("querying updated row: %v", err)
	}
	if qty != 999 {
		t.Errorf("updated qty = %d, want 999", qty)
	}

	var count int
	err = pair.srcPool.QueryRow(ctx, `SELECT count(*) FROM items WHERE id = 3`).Scan(&count)
	if err != nil {
		t.Fatalf("querying deleted row: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows with id=3, got %d", count)
	}
}

func TestMerge_ConflictAborts(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Update in both source and sandbox to create conflict.
	_, err := pair.sandboxPool.Exec(ctx, `UPDATE items SET name = 'sb' WHERE id = 1`)
	if err != nil {
		t.Fatalf("sandbox update: %v", err)
	}
	_, err = pair.srcPool.Exec(ctx, `UPDATE items SET name = 'src' WHERE id = 1`)
	if err != nil {
		t.Fatalf("source update: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) == 0 {
		t.Fatal("expected at least 1 conflict")
	}

	// Merge without --force should fail.
	err = Merge(ctx, pair.srcPool, diff, pair.tables, MergeOpts{})
	if err == nil {
		t.Fatal("expected error for conflicts without force")
	}
}

func TestMerge_ForceSkipsConflicts(t *testing.T) {
	skipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pair := createTestDBPair(t, ctx)
	defer pair.cleanup()

	// Update id=1 in both (conflict), insert id=99 in sandbox only (clean).
	_, err := pair.sandboxPool.Exec(ctx, `UPDATE items SET name = 'sb' WHERE id = 1`)
	if err != nil {
		t.Fatalf("sandbox update: %v", err)
	}
	_, err = pair.srcPool.Exec(ctx, `UPDATE items SET name = 'src' WHERE id = 1`)
	if err != nil {
		t.Fatalf("source update: %v", err)
	}
	_, err = pair.sandboxPool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (99, 'clean', 1)`)
	if err != nil {
		t.Fatalf("sandbox insert: %v", err)
	}

	diff, err := Diff(ctx, pair.srcPool, pair.sandboxPool, pair.tables)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if len(diff.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(diff.Conflicts))
	}

	// Merge with --force should succeed (skipping the conflicted row).
	err = Merge(ctx, pair.srcPool, diff, pair.tables, MergeOpts{Force: true, MaxRetries: 1})
	if err != nil {
		t.Fatalf("Merge with force: %v", err)
	}

	// The clean insert should have been applied.
	var name string
	err = pair.srcPool.QueryRow(ctx, `SELECT name FROM items WHERE id = 99`).Scan(&name)
	if err != nil {
		t.Fatalf("querying clean insert: %v", err)
	}
	if name != "clean" {
		t.Errorf("name = %q, want clean", name)
	}

	// The conflicted row should still have the source value.
	err = pair.srcPool.QueryRow(ctx, `SELECT name FROM items WHERE id = 1`).Scan(&name)
	if err != nil {
		t.Fatalf("querying conflicted row: %v", err)
	}
	if name != "src" {
		t.Errorf("conflicted row name = %q, want src (unchanged)", name)
	}
}

// --- helpers ---

func m(kvs ...any) map[string]any {
	result := make(map[string]any, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		result[kvs[i].(string)] = kvs[i+1]
	}
	return result
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

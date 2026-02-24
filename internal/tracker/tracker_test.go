package tracker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Unit tests for collapse (no DB required) ---

func TestCollapse_SingleInsert(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpInsert, PKCols: []string{"id"}, PKVals: m("id", 1), NewRow: m("id", 1, "name", "alice")},
		last:  changeRow{Table: "t", Op: OpInsert, PKCols: []string{"id"}, PKVals: m("id", 1), NewRow: m("id", 1, "name", "alice")},
		ops:   []ChangeOp{OpInsert},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpInsert {
		t.Errorf("Op = %q, want INSERT", c.Op)
	}
	if c.OldRow != nil {
		t.Error("expected nil OldRow for INSERT")
	}
}

func TestCollapse_SingleUpdate(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpUpdate, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice"), NewRow: m("id", 1, "name", "bob")},
		last:  changeRow{Table: "t", Op: OpUpdate, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice"), NewRow: m("id", 1, "name", "bob")},
		ops:   []ChangeOp{OpUpdate},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpUpdate {
		t.Errorf("Op = %q, want UPDATE", c.Op)
	}
}

func TestCollapse_SingleDelete(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice")},
		last:  changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice")},
		ops:   []ChangeOp{OpDelete},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpDelete {
		t.Errorf("Op = %q, want DELETE", c.Op)
	}
	if c.NewRow != nil {
		t.Error("expected nil NewRow for DELETE")
	}
}

func TestCollapse_InsertThenDelete(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpInsert, PKCols: []string{"id"}, PKVals: m("id", 1), NewRow: m("id", 1, "name", "alice")},
		last:  changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice")},
		ops:   []ChangeOp{OpInsert, OpDelete},
	}
	c := collapse(a)
	if c != nil {
		t.Fatalf("expected nil (no-op), got %+v", c)
	}
}

func TestCollapse_InsertThenUpdate(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpInsert, PKCols: []string{"id"}, PKVals: m("id", 1), NewRow: m("id", 1, "name", "alice")},
		last:  changeRow{Table: "t", Op: OpUpdate, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice"), NewRow: m("id", 1, "name", "bob")},
		ops:   []ChangeOp{OpInsert, OpUpdate},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpInsert {
		t.Errorf("Op = %q, want INSERT", c.Op)
	}
	if c.NewRow["name"] != "bob" {
		t.Errorf("NewRow[name] = %v, want bob", c.NewRow["name"])
	}
}

func TestCollapse_UpdateThenUpdate(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpUpdate, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice"), NewRow: m("id", 1, "name", "bob")},
		last:  changeRow{Table: "t", Op: OpUpdate, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "bob"), NewRow: m("id", 1, "name", "charlie")},
		ops:   []ChangeOp{OpUpdate, OpUpdate},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpUpdate {
		t.Errorf("Op = %q, want UPDATE", c.Op)
	}
	if c.OldRow["name"] != "alice" {
		t.Errorf("OldRow[name] = %v, want alice (first old_row)", c.OldRow["name"])
	}
	if c.NewRow["name"] != "charlie" {
		t.Errorf("NewRow[name] = %v, want charlie (last new_row)", c.NewRow["name"])
	}
}

func TestCollapse_UpdateThenDelete(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpUpdate, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice"), NewRow: m("id", 1, "name", "bob")},
		last:  changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "bob")},
		ops:   []ChangeOp{OpUpdate, OpDelete},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpDelete {
		t.Errorf("Op = %q, want DELETE", c.Op)
	}
	if c.OldRow["name"] != "alice" {
		t.Errorf("OldRow[name] = %v, want alice (first old_row)", c.OldRow["name"])
	}
}

func TestCollapse_DeleteThenInsert(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice")},
		last:  changeRow{Table: "t", Op: OpInsert, PKCols: []string{"id"}, PKVals: m("id", 1), NewRow: m("id", 1, "name", "bob")},
		ops:   []ChangeOp{OpDelete, OpInsert},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpUpdate {
		t.Errorf("Op = %q, want UPDATE", c.Op)
	}
	if c.OldRow["name"] != "alice" {
		t.Errorf("OldRow[name] = %v, want alice", c.OldRow["name"])
	}
	if c.NewRow["name"] != "bob" {
		t.Errorf("NewRow[name] = %v, want bob", c.NewRow["name"])
	}
}

func TestCollapse_DeleteThenDelete(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice")},
		last:  changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice")},
		ops:   []ChangeOp{OpDelete, OpDelete},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpDelete {
		t.Errorf("Op = %q, want DELETE", c.Op)
	}
}

func TestCollapse_DeleteThenInsertThenUpdate(t *testing.T) {
	a := &changeAccum{
		first: changeRow{Table: "t", Op: OpDelete, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "alice")},
		last:  changeRow{Table: "t", Op: OpUpdate, PKCols: []string{"id"}, PKVals: m("id", 1), OldRow: m("id", 1, "name", "bob"), NewRow: m("id", 1, "name", "charlie")},
		ops:   []ChangeOp{OpDelete, OpInsert, OpUpdate},
	}
	c := collapse(a)
	if c == nil {
		t.Fatal("expected non-nil change")
	}
	if c.Op != OpUpdate {
		t.Errorf("Op = %q, want UPDATE", c.Op)
	}
	if c.OldRow["name"] != "alice" {
		t.Errorf("OldRow[name] = %v, want alice (original)", c.OldRow["name"])
	}
	if c.NewRow["name"] != "charlie" {
		t.Errorf("NewRow[name] = %v, want charlie (final)", c.NewRow["name"])
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

// createTestDB creates a temporary database with a test table and returns a pool to it.
// The caller must call the returned cleanup function when done.
func createTestDB(t *testing.T, ctx context.Context) (*pgxpool.Pool, func()) {
	t.Helper()

	adminPool, err := pgxpool.New(ctx, adminDSN())
	if err != nil {
		t.Fatalf("connecting to admin: %v", err)
	}

	dbName := fmt.Sprintf("test_tracker_%d", time.Now().UnixNano())
	_, err = adminPool.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName))
	if err != nil {
		adminPool.Close()
		t.Fatalf("creating test db: %v", err)
	}

	user := envOr("DST_USER", "devuser")
	pass := envOr("DST_PASSWORD", "devpass")
	host := envOr("DST_HOST", "localhost")
	port := envOr("DST_PORT", "15432")
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, dbName)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		_, _ = adminPool.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName))
		adminPool.Close()
		t.Fatalf("connecting to test db: %v", err)
	}

	// Create a test table with PK.
	_, err = pool.Exec(ctx, `
		CREATE TABLE items (
			id   INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			qty  INTEGER NOT NULL DEFAULT 0
		)`)
	if err != nil {
		pool.Close()
		_, _ = adminPool.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName))
		adminPool.Close()
		t.Fatalf("creating test table: %v", err)
	}

	cleanup := func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, dbName))
		adminPool.Close()
	}
	return pool, cleanup
}

func TestInstall_CreatesTablesAndTriggers(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	if err := Install(ctx, pool, []string{"items"}); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// Verify _dendrite_changes table exists.
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = '_dendrite_changes'
		)`).Scan(&exists)
	if err != nil {
		t.Fatalf("checking _dendrite_changes: %v", err)
	}
	if !exists {
		t.Error("_dendrite_changes table not created")
	}

	// Verify trigger exists on items table.
	var triggerExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.triggers
			WHERE trigger_name = '_dendrite_track'
			  AND event_object_table = 'items'
		)`).Scan(&triggerExists)
	if err != nil {
		t.Fatalf("checking trigger: %v", err)
	}
	if !triggerExists {
		t.Error("trigger _dendrite_track not created on items")
	}
}

func TestInstall_ThenReadChanges_Insert(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	if err := Install(ctx, pool, []string{"items"}); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// Insert a row — trigger should record it.
	_, err := pool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10)`)
	if err != nil {
		t.Fatalf("inserting: %v", err)
	}

	changes, err := ReadChanges(ctx, pool)
	if err != nil {
		t.Fatalf("ReadChanges: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}

	c := changes[0]
	if c.Op != OpInsert {
		t.Errorf("Op = %q, want INSERT", c.Op)
	}
	if c.Table != "items" {
		t.Errorf("Table = %q, want items", c.Table)
	}
	if c.OldRow != nil {
		t.Error("expected nil OldRow for INSERT")
	}
	if c.NewRow == nil {
		t.Fatal("expected non-nil NewRow for INSERT")
	}
}

func TestInstall_ThenReadChanges_Update(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	// Seed a row before installing triggers.
	_, err := pool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10)`)
	if err != nil {
		t.Fatalf("seeding: %v", err)
	}

	if err := Install(ctx, pool, []string{"items"}); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// Update the row.
	_, err = pool.Exec(ctx, `UPDATE items SET name = 'gadget' WHERE id = 1`)
	if err != nil {
		t.Fatalf("updating: %v", err)
	}

	changes, err := ReadChanges(ctx, pool)
	if err != nil {
		t.Fatalf("ReadChanges: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}

	c := changes[0]
	if c.Op != OpUpdate {
		t.Errorf("Op = %q, want UPDATE", c.Op)
	}
	if c.OldRow == nil || c.NewRow == nil {
		t.Fatal("expected both OldRow and NewRow for UPDATE")
	}
}

func TestInstall_ThenReadChanges_Delete(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	// Seed a row before installing triggers.
	_, err := pool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10)`)
	if err != nil {
		t.Fatalf("seeding: %v", err)
	}

	if err := Install(ctx, pool, []string{"items"}); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// Delete the row.
	_, err = pool.Exec(ctx, `DELETE FROM items WHERE id = 1`)
	if err != nil {
		t.Fatalf("deleting: %v", err)
	}

	changes, err := ReadChanges(ctx, pool)
	if err != nil {
		t.Fatalf("ReadChanges: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}

	c := changes[0]
	if c.Op != OpDelete {
		t.Errorf("Op = %q, want DELETE", c.Op)
	}
	if c.OldRow == nil {
		t.Fatal("expected non-nil OldRow for DELETE")
	}
	if c.NewRow != nil {
		t.Error("expected nil NewRow for DELETE")
	}
}

func TestReadChanges_Collapsing_InsertThenUpdate(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	if err := Install(ctx, pool, []string{"items"}); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// INSERT then UPDATE on same row.
	_, err := pool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10)`)
	if err != nil {
		t.Fatalf("inserting: %v", err)
	}
	_, err = pool.Exec(ctx, `UPDATE items SET name = 'gadget' WHERE id = 1`)
	if err != nil {
		t.Fatalf("updating: %v", err)
	}

	changes, err := ReadChanges(ctx, pool)
	if err != nil {
		t.Fatalf("ReadChanges: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 collapsed change, got %d", len(changes))
	}

	c := changes[0]
	if c.Op != OpInsert {
		t.Errorf("Op = %q, want INSERT (collapsed INSERT+UPDATE)", c.Op)
	}
	// The final new_row should have the updated name.
	if name, ok := c.NewRow["name"].(string); !ok || name != "gadget" {
		t.Errorf("NewRow[name] = %v, want gadget", c.NewRow["name"])
	}
}

func TestReadChanges_Collapsing_InsertThenDelete(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	if err := Install(ctx, pool, []string{"items"}); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// INSERT then DELETE — should collapse to no-op.
	_, err := pool.Exec(ctx, `INSERT INTO items (id, name, qty) VALUES (1, 'widget', 10)`)
	if err != nil {
		t.Fatalf("inserting: %v", err)
	}
	_, err = pool.Exec(ctx, `DELETE FROM items WHERE id = 1`)
	if err != nil {
		t.Fatalf("deleting: %v", err)
	}

	changes, err := ReadChanges(ctx, pool)
	if err != nil {
		t.Fatalf("ReadChanges: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("expected 0 changes (no-op), got %d", len(changes))
	}
}

func TestReadChanges_NoChangesTable(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	// Don't install — ReadChanges should return a clear error.
	_, err := ReadChanges(ctx, pool)
	if err == nil {
		t.Fatal("expected error for missing _dendrite_changes")
	}
}

func TestReadChanges_Empty(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	if err := Install(ctx, pool, []string{"items"}); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// No DML — should return empty slice.
	changes, err := ReadChanges(ctx, pool)
	if err != nil {
		t.Fatalf("ReadChanges: %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("expected 0 changes, got %d", len(changes))
	}
}

// m is a test helper that builds a map[string]any from alternating key/value pairs.
func m(kvs ...any) map[string]any {
	result := make(map[string]any, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		result[kvs[i].(string)] = kvs[i+1]
	}
	return result
}

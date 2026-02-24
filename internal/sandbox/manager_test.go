package sandbox

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Unit tests (no DB required) ---

func TestRandomID(t *testing.T) {
	id := randomID()
	if len(id) != 16 {
		t.Fatalf("expected 16-char hex, got %d chars: %q", len(id), id)
	}
	// Should be unique.
	id2 := randomID()
	if id == id2 {
		t.Fatal("two calls returned the same ID")
	}
}

func TestRandomPassword(t *testing.T) {
	pw := randomPassword()
	if len(pw) != 32 {
		t.Fatalf("expected 32-char hex, got %d chars: %q", len(pw), pw)
	}
}

func TestPgIdent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"sandbox_abc123", `"sandbox_abc123"`},
		{`has"quote`, `"has""quote"`},
		{"simple", `"simple"`},
	}
	for _, tt := range tests {
		got := pgIdent(tt.input)
		if got != tt.want {
			t.Errorf("pgIdent(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPgLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"password123", `'password123'`},
		{"it's", `'it''s'`},
		{"clean", `'clean'`},
	}
	for _, tt := range tests {
		got := pgLiteral(tt.input)
		if got != tt.want {
			t.Errorf("pgLiteral(%q) = %q, want %q", tt.input, got, tt.want)
		}
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

func TestNewManager(t *testing.T) {
	skipIfNoPostgres(t)

	mgr, err := NewManager(adminDSN())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	if mgr.host == "" {
		t.Error("expected host to be set")
	}
	if mgr.port == "" || mgr.port == "0" {
		t.Error("expected port to be set")
	}
}

func TestNewManager_BadDSN(t *testing.T) {
	_, err := NewManager("postgres://bad:bad@localhost:1/nope")
	if err == nil {
		t.Fatal("expected error for unreachable DSN")
	}
}

func TestLifecycle_CreateListDestroy(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr, err := NewManager(adminDSN())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	// Create a sandbox with no copy specs (empty DB, just role + database).
	sb, err := mgr.Create(ctx, adminDSN(), nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if sb.ID == "" {
		t.Error("expected non-empty ID")
	}
	if sb.Database != "sandbox_"+sb.ID {
		t.Errorf("Database = %q, want %q", sb.Database, "sandbox_"+sb.ID)
	}
	if sb.Role != sb.Database {
		t.Errorf("Role = %q, want %q", sb.Role, sb.Database)
	}
	if sb.Password == "" {
		t.Error("expected non-empty Password")
	}
	if sb.DSN == "" {
		t.Error("expected non-empty DSN")
	}

	t.Logf("created sandbox %s", sb.ID)

	// Verify it appears in List.
	sandboxes, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	found := false
	for _, s := range sandboxes {
		if s.ID == sb.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("sandbox %s not found in List (got %d sandboxes)", sb.ID, len(sandboxes))
	}

	// Verify the database actually exists in pg_database.
	var exists bool
	err = mgr.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`,
		sb.Database,
	).Scan(&exists)
	if err != nil {
		t.Fatalf("checking pg_database: %v", err)
	}
	if !exists {
		t.Errorf("database %s does not exist in pg_database", sb.Database)
	}

	// Verify the role exists.
	err = mgr.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)`,
		sb.Role,
	).Scan(&exists)
	if err != nil {
		t.Fatalf("checking pg_roles: %v", err)
	}
	if !exists {
		t.Errorf("role %s does not exist in pg_roles", sb.Role)
	}

	// Destroy it.
	if err := mgr.Destroy(ctx, sb.ID); err != nil {
		t.Fatalf("Destroy: %v", err)
	}

	t.Logf("destroyed sandbox %s", sb.ID)

	// Verify it no longer appears in List.
	sandboxes, err = mgr.List(ctx)
	if err != nil {
		t.Fatalf("List after destroy: %v", err)
	}
	for _, s := range sandboxes {
		if s.ID == sb.ID {
			t.Errorf("sandbox %s still appears in List after destroy", sb.ID)
		}
	}

	// Verify database is gone.
	err = mgr.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`,
		sb.Database,
	).Scan(&exists)
	if err != nil {
		t.Fatalf("checking pg_database after destroy: %v", err)
	}
	if exists {
		t.Errorf("database %s still exists after destroy", sb.Database)
	}

	// Verify role is gone.
	err = mgr.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)`,
		sb.Role,
	).Scan(&exists)
	if err != nil {
		t.Fatalf("checking pg_roles after destroy: %v", err)
	}
	if exists {
		t.Errorf("role %s still exists after destroy", sb.Role)
	}
}

func TestDestroy_Idempotent(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mgr, err := NewManager(adminDSN())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	// Destroying a non-existent sandbox should not error
	// (DROP IF EXISTS handles this).
	if err := mgr.Destroy(ctx, "0000000000000000"); err != nil {
		t.Fatalf("Destroy non-existent: %v", err)
	}
}

func TestList_Empty(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mgr, err := NewManager(adminDSN())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	// Clean up any leftover sandboxes from previous test runs.
	sandboxes, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, sb := range sandboxes {
		_ = mgr.Destroy(ctx, sb.ID)
	}

	// Now list should be empty.
	sandboxes, err = mgr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(sandboxes) != 0 {
		t.Errorf("expected 0 sandboxes, got %d", len(sandboxes))
	}
}

func TestCreateMultiple(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr, err := NewManager(adminDSN())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	// Create two sandboxes.
	sb1, err := mgr.Create(ctx, adminDSN(), nil)
	if err != nil {
		t.Fatalf("Create 1: %v", err)
	}
	sb2, err := mgr.Create(ctx, adminDSN(), nil)
	if err != nil {
		t.Fatalf("Create 2: %v", err)
	}

	if sb1.ID == sb2.ID {
		t.Fatal("two sandboxes have the same ID")
	}

	// Both should appear in List.
	sandboxes, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	ids := make(map[string]bool)
	for _, s := range sandboxes {
		ids[s.ID] = true
	}
	if !ids[sb1.ID] {
		t.Errorf("sandbox %s not in list", sb1.ID)
	}
	if !ids[sb2.ID] {
		t.Errorf("sandbox %s not in list", sb2.ID)
	}

	// Cleanup.
	if err := mgr.Destroy(ctx, sb1.ID); err != nil {
		t.Errorf("Destroy %s: %v", sb1.ID, err)
	}
	if err := mgr.Destroy(ctx, sb2.ID); err != nil {
		t.Errorf("Destroy %s: %v", sb2.ID, err)
	}
}

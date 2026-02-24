package tracker

import (
	"context"
	"testing"
	"time"
)

func TestWriteReadMeta_RoundTrip(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	srcDSN := "postgres://user:pass@host:5432/source"
	tables := []string{"items", "orders", "users"}

	if err := WriteMeta(ctx, pool, srcDSN, tables); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}

	gotDSN, gotTables, err := ReadMeta(ctx, pool)
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}

	if gotDSN != srcDSN {
		t.Errorf("srcDSN = %q, want %q", gotDSN, srcDSN)
	}
	if len(gotTables) != len(tables) {
		t.Fatalf("tables len = %d, want %d", len(gotTables), len(tables))
	}
	for i, want := range tables {
		if gotTables[i] != want {
			t.Errorf("tables[%d] = %q, want %q", i, gotTables[i], want)
		}
	}
}

func TestWriteMeta_Upsert(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	// Write once.
	if err := WriteMeta(ctx, pool, "dsn1", []string{"a"}); err != nil {
		t.Fatalf("WriteMeta 1: %v", err)
	}

	// Write again — should upsert, not error.
	if err := WriteMeta(ctx, pool, "dsn2", []string{"b", "c"}); err != nil {
		t.Fatalf("WriteMeta 2: %v", err)
	}

	gotDSN, gotTables, err := ReadMeta(ctx, pool)
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	if gotDSN != "dsn2" {
		t.Errorf("srcDSN = %q, want dsn2", gotDSN)
	}
	if len(gotTables) != 2 || gotTables[0] != "b" || gotTables[1] != "c" {
		t.Errorf("tables = %v, want [b c]", gotTables)
	}
}

func TestReadMeta_NoTable(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	// Don't call WriteMeta — ReadMeta should return a clear error.
	_, _, err := ReadMeta(ctx, pool)
	if err == nil {
		t.Fatal("expected error for missing _dendrite_meta")
	}
}

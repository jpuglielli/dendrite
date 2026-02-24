package tracker

import (
	"context"
	"testing"
	"time"
)

func TestDiscoverPKs_SingleColumnPK(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	pks, err := DiscoverPKs(ctx, pool, []string{"items"})
	if err != nil {
		t.Fatalf("DiscoverPKs: %v", err)
	}

	cols, ok := pks["items"]
	if !ok {
		t.Fatal("no entry for items")
	}
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("PK cols = %v, want [id]", cols)
	}
}

func TestDiscoverPKs_CompositePK(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	// Create a table with composite PK.
	_, err := pool.Exec(ctx, `
		CREATE TABLE order_items (
			order_id INTEGER NOT NULL,
			item_id  INTEGER NOT NULL,
			qty      INTEGER NOT NULL DEFAULT 1,
			PRIMARY KEY (order_id, item_id)
		)`)
	if err != nil {
		t.Fatalf("creating composite PK table: %v", err)
	}

	pks, err := DiscoverPKs(ctx, pool, []string{"order_items"})
	if err != nil {
		t.Fatalf("DiscoverPKs: %v", err)
	}

	cols := pks["order_items"]
	if len(cols) != 2 || cols[0] != "order_id" || cols[1] != "item_id" {
		t.Errorf("PK cols = %v, want [order_id, item_id]", cols)
	}
}

func TestDiscoverPKs_NoPK(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	// Create a table without PK.
	_, err := pool.Exec(ctx, `CREATE TABLE no_pk (val TEXT)`)
	if err != nil {
		t.Fatalf("creating no_pk table: %v", err)
	}

	_, err = DiscoverPKs(ctx, pool, []string{"no_pk"})
	if err == nil {
		t.Fatal("expected error for table without PK")
	}
}

func TestDiscoverPKs_MultipleTables(t *testing.T) {
	skipIfNoPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, cleanup := createTestDB(t, ctx)
	defer cleanup()

	_, err := pool.Exec(ctx, `CREATE TABLE other (code TEXT PRIMARY KEY, label TEXT)`)
	if err != nil {
		t.Fatalf("creating other table: %v", err)
	}

	pks, err := DiscoverPKs(ctx, pool, []string{"items", "other"})
	if err != nil {
		t.Fatalf("DiscoverPKs: %v", err)
	}

	if len(pks) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(pks))
	}
	if pks["items"][0] != "id" {
		t.Errorf("items PK = %v, want [id]", pks["items"])
	}
	if pks["other"][0] != "code" {
		t.Errorf("other PK = %v, want [code]", pks["other"])
	}
}

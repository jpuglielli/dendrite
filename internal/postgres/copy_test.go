package postgres

import "testing"

func TestTableNames(t *testing.T) {
	specs := []CopySpec{
		{Query: "SELECT * FROM a", Table: "a"},
		{Query: "SELECT * FROM b", Table: "b"},
		{Query: "SELECT * FROM c", Table: "c"},
	}
	names := TableNames(specs)
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}
	for i, want := range []string{"a", "b", "c"} {
		if names[i] != want {
			t.Errorf("names[%d] = %q, want %q", i, names[i], want)
		}
	}
}

func TestTableNames_Empty(t *testing.T) {
	names := TableNames(nil)
	if len(names) != 0 {
		t.Fatalf("expected 0 names, got %d", len(names))
	}
}

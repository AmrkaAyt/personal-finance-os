package migratex

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDirSortsAndReplaces(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "migrations", "postgres")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000002_second.sql"), []byte("CREATE TABLE {{NAME}} (id int);"), 0o644); err != nil {
		t.Fatalf("write second: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000001_first.sql"), []byte("CREATE TABLE test (id int);"), 0o644); err != nil {
		t.Fatalf("write first: %v", err)
	}

	migrations, err := LoadDir(root, "migrations/postgres", map[string]string{"{{NAME}}": "other"})
	if err != nil {
		t.Fatalf("load dir: %v", err)
	}
	if len(migrations) != 2 {
		t.Fatalf("expected 2 migrations, got %d", len(migrations))
	}
	if migrations[0].Version != "000001" || migrations[1].Version != "000002" {
		t.Fatalf("unexpected migration order: %#v", migrations)
	}
	if len(migrations[1].Statements) != 1 || migrations[1].Statements[0] != "CREATE TABLE other (id int)" {
		t.Fatalf("unexpected statements: %#v", migrations[1].Statements)
	}
}

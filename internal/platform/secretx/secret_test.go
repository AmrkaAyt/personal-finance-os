package secretx

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveEnv(t *testing.T) {
	t.Setenv("SECRETX_TEST_ENV", "secret-value")
	value, err := Resolve("env:SECRETX_TEST_ENV")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if value != "secret-value" {
		t.Fatalf("expected secret-value, got %q", value)
	}
}

func TestResolveFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret.txt")
	if err := os.WriteFile(path, []byte(" file-secret \n"), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	value, err := Resolve("file:" + path)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if value != "file-secret" {
		t.Fatalf("expected file-secret, got %q", value)
	}
}

func TestRefOrEnv(t *testing.T) {
	t.Setenv("SECRETX_FALLBACK", "fallback")
	if actual := RefOrEnv("", "SECRETX_FALLBACK"); actual != "env:SECRETX_FALLBACK" {
		t.Fatalf("expected env ref, got %q", actual)
	}
	if actual := RefOrEnv("file:/run/secrets/x", "SECRETX_FALLBACK"); actual != "file:/run/secrets/x" {
		t.Fatalf("expected explicit ref, got %q", actual)
	}
}

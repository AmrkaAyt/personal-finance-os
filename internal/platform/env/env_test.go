package env

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStringLoadsEnvFile(t *testing.T) {
	tempDir := t.TempDir()
	envPath := filepath.Join(tempDir, ".env.test")
	if err := os.WriteFile(envPath, []byte("TEST_ENV_KEY=loaded-value\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	t.Setenv("ENV_FILE", envPath)
	t.Setenv("TEST_ENV_KEY", "")
	_ = os.Unsetenv("TEST_ENV_KEY")

	sharedOnce = sync.Once{}
	serviceLoadMu = sync.Mutex{}
	loadedService = ""
	t.Cleanup(func() {
		sharedOnce = sync.Once{}
		serviceLoadMu = sync.Mutex{}
		loadedService = ""
		_ = os.Unsetenv("TEST_ENV_KEY")
	})

	LoadService("test-service")
	got := String("TEST_ENV_KEY", "fallback")
	if got != "loaded-value" {
		t.Fatalf("String() = %q, want %q", got, "loaded-value")
	}
}

func TestNormalizeValue(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		` plain `:       "plain",
		`"quoted"`:      "quoted",
		`'single'`:      "single",
		`" spaced "`:    " spaced ",
		`unquoted`:      "unquoted",
		`"with=equals"`: "with=equals",
	}

	for input, want := range cases {
		if got := normalizeValue(input); got != want {
			t.Fatalf("normalizeValue(%q) = %q, want %q", input, got, want)
		}
	}
}

package secureenv

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestEnforceSkipsInLocalMode(t *testing.T) {
	t.Setenv("APP_ENV", "local")
	err := Enforce("test-service", slog.New(slog.NewTextHandler(io.Discard, nil)),
		RequireNonEmpty("JWT_SECRET", ""),
	)
	if err != nil {
		t.Fatalf("expected nil error in local mode, got %v", err)
	}
}

func TestEnforceFailsOutsideLocal(t *testing.T) {
	previous := os.Getenv("APP_ENV")
	t.Setenv("APP_ENV", "production")
	t.Cleanup(func() {
		_ = os.Setenv("APP_ENV", previous)
	})

	err := Enforce("test-service", slog.New(slog.NewTextHandler(io.Discard, nil)),
		RequireNonEmpty("JWT_SECRET", ""),
	)
	if err == nil {
		t.Fatal("expected error in production mode")
	}
}

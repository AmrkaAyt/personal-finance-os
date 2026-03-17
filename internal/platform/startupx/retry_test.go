package startupx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestRetryValueEventuallySucceeds(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	attempts := 0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	value, err := RetryValueWithConfig(ctx, logger, "eventual success", Config{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		AttemptTimeout: 50 * time.Millisecond,
	}, func(context.Context) (string, error) {
		attempts++
		if attempts < 3 {
			return "", errors.New("not yet ready")
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("RetryValueWithConfig returned error: %v", err)
	}
	if value != "ok" {
		t.Fatalf("RetryValueWithConfig value = %q, want ok", value)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryValueStopsOnContextDeadline(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	_, err := RetryValueWithConfig(ctx, logger, "deadline", Config{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		AttemptTimeout: 15 * time.Millisecond,
	}, func(context.Context) (int, error) {
		return 0, errors.New("still down")
	})
	if err == nil {
		t.Fatal("RetryValueWithConfig error = nil, want deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RetryValueWithConfig error = %v, want context deadline exceeded", err)
	}
}

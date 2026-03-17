package startupx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type Config struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	AttemptTimeout time.Duration
}

func Retry(ctx context.Context, logger *slog.Logger, operation string, fn func(context.Context) error) error {
	_, err := RetryValue(ctx, logger, operation, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

func RetryValue[T any](ctx context.Context, logger *slog.Logger, operation string, fn func(context.Context) (T, error)) (T, error) {
	return RetryValueWithConfig(ctx, logger, operation, Config{}, fn)
}

func RetryWithConfig(ctx context.Context, logger *slog.Logger, operation string, cfg Config, fn func(context.Context) error) error {
	_, err := RetryValueWithConfig(ctx, logger, operation, cfg, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

func RetryValueWithConfig[T any](ctx context.Context, logger *slog.Logger, operation string, cfg Config, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	if logger == nil {
		logger = slog.Default()
	}
	cfg = withDefaults(cfg)
	startedAt := time.Now()
	backoff := cfg.InitialBackoff
	var lastErr error

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, wrapError(operation, lastErr, err)
		}

		attemptCtx, cancel := withAttemptTimeout(ctx, cfg.AttemptTimeout)
		value, err := fn(attemptCtx)
		cancel()
		if err == nil {
			if attempt > 1 {
				logger.Info("startup dependency became ready", "operation", operation, "attempts", attempt, "elapsed", time.Since(startedAt).String())
			}
			return value, nil
		}

		lastErr = err
		if ctx.Err() != nil {
			return zero, wrapError(operation, lastErr, ctx.Err())
		}

		logger.Warn("startup dependency not ready, retrying", "operation", operation, "attempt", attempt, "backoff", backoff.String(), "error", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, wrapError(operation, lastErr, ctx.Err())
		case <-timer.C:
		}

		backoff *= 2
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}
}

func withDefaults(cfg Config) Config {
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 250 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 2 * time.Second
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = 5 * time.Second
	}
	return cfg
}

func withAttemptTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ctx, func() {}
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	return context.WithTimeout(ctx, timeout)
}

func wrapError(operation string, lastErr, ctxErr error) error {
	switch {
	case lastErr != nil && ctxErr != nil:
		return fmt.Errorf("%s: %w", operation, errors.Join(lastErr, ctxErr))
	case lastErr != nil:
		return fmt.Errorf("%s: %w", operation, lastErr)
	case ctxErr != nil:
		return fmt.Errorf("%s: %w", operation, ctxErr)
	default:
		return fmt.Errorf("%s: startup failed", operation)
	}
}

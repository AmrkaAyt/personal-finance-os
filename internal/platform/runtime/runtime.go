package runtime

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type BackgroundFunc func(context.Context, *slog.Logger) error

type Config struct {
	Name            string
	HTTPAddr        string
	Handler         http.Handler
	Logger          *slog.Logger
	ShutdownTimeout time.Duration
	Background      []BackgroundFunc
}

func Run(cfg Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}

	var server *http.Server
	if cfg.Handler != nil {
		server = &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           cfg.Handler,
			ReadHeaderTimeout: 5 * time.Second,
		}
	}

	errCh := make(chan error, len(cfg.Background)+1)
	var wg sync.WaitGroup

	if server != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg.Logger.Info("http server starting", "addr", cfg.HTTPAddr)
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}

	for _, background := range cfg.Background {
		bg := background
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bg(ctx, cfg.Logger); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errCh:
		runErr = err
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if server != nil {
		cfg.Logger.Info("http server shutting down")
		_ = server.Shutdown(shutdownCtx)
	}

	wg.Wait()
	if runErr != nil {
		cfg.Logger.Error("service stopped with error", "error", runErr)
	}
	return runErr
}

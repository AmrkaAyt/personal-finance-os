package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/ws"
)

func main() {
	const serviceName = "realtime-gateway"

	logger := logging.New(serviceName)
	redisAddr := env.String("REDIS_ADDR", "localhost:6379")
	hub := ws.NewHub(logger)
	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)

	mux.HandleFunc("GET /ws", hub.ServeWS)
	mux.HandleFunc("GET /api/v1/presence", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]any{"connections": hub.Count(), "redis_addr": redisAddr})
	})

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8088"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			func(ctx context.Context, logger *slog.Logger) error {
				ticker := time.NewTicker(10 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return nil
					case <-ticker.C:
						hub.BroadcastJSON(map[string]any{"service": serviceName, "connections": hub.Count()})
						logger.Info("presence broadcast", "connections", hub.Count(), "redis_addr", redisAddr)
					}
				}
			},
		},
	}); err != nil {
		panic(err)
	}
}

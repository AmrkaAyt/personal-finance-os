package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/jwtx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/rbac"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/ws"
)

func main() {
	const serviceName = "api-gateway"

	logger := logging.New(serviceName)
	manager := jwtx.NewManager(env.String("JWT_SECRET", "dev-secret"), serviceName, 15*time.Minute, 24*time.Hour)
	hub := ws.NewHub(logger)
	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)

	mux.HandleFunc("GET /api/v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"message": "api-gateway ready"})
	})
	mux.Handle("GET /api/v1/profile", jwtx.Middleware(manager, true)(rbac.RequireRoles("owner", "member", "advisor_readonly")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, _ := jwtx.ClaimsFromContext(r.Context())
		httpx.JSON(w, http.StatusOK, map[string]any{
			"user_id": claims.Subject,
			"roles":   claims.Roles,
		})
	}))))
	mux.HandleFunc("GET /ws", hub.ServeWS)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8080"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			heartbeatLoop(serviceName, hub),
		},
	}); err != nil {
		panic(err)
	}
}

func heartbeatLoop(service string, hub *ws.Hub) runtime.BackgroundFunc {
	return func(ctx context.Context, logger *slog.Logger) error {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				hub.BroadcastJSON(map[string]any{"service": service, "status": "alive", "clients": hub.Count()})
				logger.Info("websocket heartbeat broadcast", "clients", hub.Count())
			}
		}
	}
}

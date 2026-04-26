package main

import (
	"context"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	authsvc "personal-finance-os/internal/auth"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/jwtx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/secureenv"
	"personal-finance-os/internal/platform/startupx"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

func main() {
	const serviceName = "auth-service"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)
	jwtSecret := env.String("JWT_SECRET", "dev-secret")
	allowSeededUsers := env.Bool("AUTH_ALLOW_SEEDED_USERS", true)
	if err := secureenv.Enforce(serviceName, logger,
		secureenv.RequireNonEmpty("JWT_SECRET", jwtSecret),
		secureenv.RejectAnyOf("JWT_SECRET", jwtSecret, "dev-secret"),
		secureenv.RequireFalse("AUTH_ALLOW_SEEDED_USERS", allowSeededUsers),
	); err != nil {
		panic(err)
	}
	manager := jwtx.NewManager(jwtSecret, serviceName, 15*time.Minute, 7*24*time.Hour)
	sessions := authsvc.SessionStore(authsvc.NewMemorySessionStore())
	if redisAddr := env.String("REDIS_ADDR", ""); redisAddr != "" {
		client := redis.NewClient(&redis.Options{Addr: redisAddr})
		startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
		if err := startupx.Retry(startupCtx, logger, "redis ping", func(ctx context.Context) error {
			return client.Ping(ctx).Err()
		}); err != nil {
			cancel()
			panic(err)
		}
		cancel()
		sessions = authsvc.NewRedisSessionStore(client, env.String("REDIS_PREFIX", "auth:sessions"))
		logger.Info("redis-backed session store configured", "addr", redisAddr)
	}
	users := map[string]authsvc.User{}
	if allowSeededUsers {
		users = authsvc.DefaultUsers()
	}
	service := authsvc.NewService(manager, sessions, users)
	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)

	mux.HandleFunc("POST /auth/login", func(w http.ResponseWriter, r *http.Request) {
		var request loginRequest
		if err := httpx.ReadJSON(r, &request); err != nil {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
			return
		}
		pair, err := service.Login(r.Context(), request.Username, request.Password)
		if err != nil {
			httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}
		httpx.JSON(w, http.StatusOK, pair)
	})
	mux.HandleFunc("POST /auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		var request refreshRequest
		if err := httpx.ReadJSON(r, &request); err != nil {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
			return
		}
		pair, err := service.Refresh(r.Context(), request.RefreshToken)
		if err != nil {
			httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}
		httpx.JSON(w, http.StatusOK, pair)
	})
	mux.Handle("GET /auth/me", jwtx.Middleware(manager, true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, _ := jwtx.ClaimsFromContext(r.Context())
		httpx.JSON(w, http.StatusOK, map[string]any{
			"user_id": claims.Subject,
			"roles":   claims.Roles,
			"type":    claims.Type,
		})
	})))

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8081"),
		Handler:  mux,
		Logger:   logger,
	}); err != nil {
		panic(err)
	}
}

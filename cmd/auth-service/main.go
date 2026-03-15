package main

import (
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	authsvc "personal-finance-os/internal/auth"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/jwtx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
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

	logger := logging.New(serviceName)
	manager := jwtx.NewManager(env.String("JWT_SECRET", "dev-secret"), serviceName, 15*time.Minute, 7*24*time.Hour)
	sessions := authsvc.SessionStore(authsvc.NewMemorySessionStore())
	if redisAddr := env.String("REDIS_ADDR", ""); redisAddr != "" {
		client := redis.NewClient(&redis.Options{Addr: redisAddr})
		sessions = authsvc.NewRedisSessionStore(client, env.String("REDIS_PREFIX", "auth:sessions"))
		logger.Info("redis-backed session store configured", "addr", redisAddr)
	}
	service := authsvc.NewService(manager, sessions, authsvc.DefaultUsers())
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

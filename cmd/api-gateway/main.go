package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/jwtx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/rbac"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/userctx"
)

var (
	readRoles  = []string{"owner", "member", "advisor_readonly"}
	writeRoles = []string{"owner", "member"}
)

type service struct {
	logger            *slog.Logger
	manager           *jwtx.Manager
	authProxy         *httputil.ReverseProxy
	ingestProxy       *httputil.ReverseProxy
	parserProxy       *httputil.ReverseProxy
	ledgerProxy       *httputil.ReverseProxy
	notificationProxy *httputil.ReverseProxy
	analyticsProxy    *httputil.ReverseProxy
	realtimeProxy     *httputil.ReverseProxy

	authURL         *url.URL
	ingestURL       *url.URL
	parserURL       *url.URL
	ledgerURL       *url.URL
	notificationURL *url.URL
	analyticsURL    *url.URL
	realtimeURL     *url.URL
}

func main() {
	const serviceName = "api-gateway"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	manager := jwtx.NewManager(env.String("JWT_SECRET", "dev-secret"), serviceName, 15*time.Minute, 7*24*time.Hour)

	svc, err := newService(logger, manager)
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)

	mux.HandleFunc("GET /api/v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]any{
			"message": "api-gateway ready",
			"targets": map[string]string{
				"auth":         svc.authURL.String(),
				"ingest":       svc.ingestURL.String(),
				"parser":       svc.parserURL.String(),
				"ledger":       svc.ledgerURL.String(),
				"notification": svc.notificationURL.String(),
				"analytics":    svc.analyticsURL.String(),
				"realtime":     svc.realtimeURL.String(),
			},
		})
	})
	mux.Handle("GET /api/v1/profile", svc.protected(http.HandlerFunc(svc.handleProfile), readRoles...))
	mux.Handle("POST /auth/login", svc.proxyHandler(svc.authProxy))
	mux.Handle("POST /auth/refresh", svc.proxyHandler(svc.authProxy))
	mux.Handle("GET /auth/me", svc.protected(svc.proxyHandler(svc.authProxy), readRoles...))

	mux.Handle("POST /imports/raw", svc.protected(svc.proxyHandler(svc.ingestProxy), writeRoles...))
	mux.Handle("GET /imports/{importID}", svc.protected(svc.proxyHandler(svc.ingestProxy), readRoles...))
	mux.Handle("GET /parser/results/{importID}", svc.protected(svc.proxyHandler(svc.parserProxy), readRoles...))

	mux.Handle("GET /api/v1/transactions", svc.protected(svc.proxyHandler(svc.ledgerProxy), readRoles...))
	mux.Handle("POST /api/v1/transactions", svc.protected(svc.proxyHandler(svc.ledgerProxy), writeRoles...))
	mux.Handle("GET /api/v1/categories", svc.protected(svc.proxyHandler(svc.ledgerProxy), readRoles...))
	mux.Handle("GET /api/v1/recurring", svc.protected(svc.proxyHandler(svc.ledgerProxy), readRoles...))

	mux.Handle("GET /api/v1/notifications/status", svc.protected(svc.proxyHandler(svc.notificationProxy), readRoles...))
	mux.Handle("POST /api/v1/notifications/telegram/demo", svc.protected(svc.proxyHandler(svc.notificationProxy), writeRoles...))
	mux.Handle("POST /api/v1/notifications/telegram/poll/once", svc.protected(svc.proxyHandler(svc.notificationProxy), writeRoles...))

	mux.Handle("GET /api/v1/analytics/projections", svc.protected(svc.proxyHandler(svc.analyticsProxy), readRoles...))
	mux.Handle("GET /api/v1/analytics/projections/daily-spend", svc.protected(svc.proxyHandler(svc.analyticsProxy), readRoles...))
	mux.Handle("GET /api/v1/analytics/projections/alerts", svc.protected(svc.proxyHandler(svc.analyticsProxy), readRoles...))
	mux.Handle("GET /api/v1/analytics/projections/summary", svc.protected(svc.proxyHandler(svc.analyticsProxy), readRoles...))

	mux.Handle("GET /api/v1/presence", svc.protected(svc.proxyHandler(svc.realtimeProxy), readRoles...))
	mux.Handle("GET /api/v1/realtime/config", svc.protected(svc.proxyHandler(svc.realtimeProxy), readRoles...))
	mux.Handle("GET /ws", svc.websocketProxy(readRoles...))

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8080"),
		Handler:  mux,
		Logger:   logger,
	}); err != nil {
		panic(err)
	}
}

func newService(logger *slog.Logger, manager *jwtx.Manager) (*service, error) {
	authURL, authProxy, err := newReverseProxy(env.String("AUTH_SERVICE_URL", "http://localhost:8081"), "auth-service", logger)
	if err != nil {
		return nil, err
	}
	ingestURL, ingestProxy, err := newReverseProxy(env.String("INGEST_SERVICE_URL", "http://localhost:8082"), "ingest-service", logger)
	if err != nil {
		return nil, err
	}
	parserURL, parserProxy, err := newReverseProxy(env.String("PARSER_SERVICE_URL", "http://localhost:8083"), "parser-service", logger)
	if err != nil {
		return nil, err
	}
	ledgerURL, ledgerProxy, err := newReverseProxy(env.String("LEDGER_SERVICE_URL", "http://localhost:8084"), "ledger-service", logger)
	if err != nil {
		return nil, err
	}
	notificationURL, notificationProxy, err := newReverseProxy(env.String("NOTIFICATION_SERVICE_URL", "http://localhost:8086"), "notification-service", logger)
	if err != nil {
		return nil, err
	}
	analyticsURL, analyticsProxy, err := newReverseProxy(env.String("ANALYTICS_SERVICE_URL", "http://localhost:8087"), "analytics-writer", logger)
	if err != nil {
		return nil, err
	}
	realtimeURL, realtimeProxy, err := newReverseProxy(env.String("REALTIME_SERVICE_URL", "http://localhost:8088"), "realtime-gateway", logger)
	if err != nil {
		return nil, err
	}

	return &service{
		logger:            logger,
		manager:           manager,
		authProxy:         authProxy,
		ingestProxy:       ingestProxy,
		parserProxy:       parserProxy,
		ledgerProxy:       ledgerProxy,
		notificationProxy: notificationProxy,
		analyticsProxy:    analyticsProxy,
		realtimeProxy:     realtimeProxy,
		authURL:           authURL,
		ingestURL:         ingestURL,
		parserURL:         parserURL,
		ledgerURL:         ledgerURL,
		notificationURL:   notificationURL,
		analyticsURL:      analyticsURL,
		realtimeURL:       realtimeURL,
	}, nil
}

func newReverseProxy(rawURL, name string, logger *slog.Logger) (*url.URL, *httputil.ReverseProxy, error) {
	target, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	baseDirector := proxy.Director
	proxy.Director = func(request *http.Request) {
		baseDirector(request)
		request.Host = target.Host
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		logger.Error("reverse proxy failed", "target", name, "url", target.String(), "error", err)
		httpx.JSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("%s unavailable", name)})
	}
	return target, proxy, nil
}

func (s *service) handleProfile(w http.ResponseWriter, r *http.Request) {
	claims, _ := jwtx.ClaimsFromContext(r.Context())
	httpx.JSON(w, http.StatusOK, map[string]any{
		"user_id": claims.Subject,
		"roles":   claims.Roles,
		"type":    claims.Type,
	})
}

func (s *service) protected(handler http.Handler, roles ...string) http.Handler {
	return jwtx.Middleware(s.manager, true)(rbac.RequireRoles(roles...)(handler))
}

func (s *service) proxyHandler(proxy *httputil.ReverseProxy) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := cloneRequest(r)
		userctx.StripUserIDQuery(request)
		if claims, ok := jwtx.ClaimsFromContext(r.Context()); ok {
			userctx.SetAuthenticatedUserID(request, claims.Subject)
			request.Header.Set("X-User-Roles", strings.Join(claims.Roles, ","))
			request.Header.Set("X-Auth-Token-Type", claims.Type)
		}
		proxy.ServeHTTP(w, request)
	})
}

func (s *service) websocketProxy(roles ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, token, err := authenticateWebSocketRequest(r, s.manager)
		if err != nil {
			httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}
		if !rbac.HasAnyRole(claims, roles...) {
			httpx.JSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
			return
		}

		request := cloneRequest(r)
		query := request.URL.Query()
		query.Set("user_id", claims.Subject)
		query.Del("access_token")
		request.URL.RawQuery = query.Encode()
		request.Header.Set("Authorization", "Bearer "+token)
		userctx.SetAuthenticatedUserID(request, claims.Subject)
		request.Header.Set("X-User-Roles", strings.Join(claims.Roles, ","))

		s.realtimeProxy.ServeHTTP(w, request)
	})
}

func authenticateWebSocketRequest(r *http.Request, manager *jwtx.Manager) (*jwtx.Claims, string, error) {
	token := bearerTokenFromRequest(r)
	if token == "" {
		return nil, "", fmt.Errorf("missing access token")
	}

	claims, err := manager.Parse(token)
	if err != nil {
		return nil, "", fmt.Errorf("invalid token")
	}
	if claims.Type != "access" {
		return nil, "", fmt.Errorf("token is not an access token")
	}
	return claims, token, nil
}

func bearerTokenFromRequest(r *http.Request) string {
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if authorization != "" {
		parts := strings.SplitN(authorization, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") && strings.TrimSpace(parts[1]) != "" {
			return strings.TrimSpace(parts[1])
		}
	}
	return strings.TrimSpace(r.URL.Query().Get("access_token"))
}

func cloneRequest(r *http.Request) *http.Request {
	cloned := r.Clone(r.Context())
	cloned.Header = r.Header.Clone()
	urlCopy := *r.URL
	cloned.URL = &urlCopy
	return cloned
}

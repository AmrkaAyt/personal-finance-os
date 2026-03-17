package main

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"personal-finance-os/internal/platform/jwtx"
)

func TestBearerTokenFromRequestPrefersHeader(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest("GET", "/ws?access_token=query-token", nil)
	request.Header.Set("Authorization", "Bearer header-token")

	if token := bearerTokenFromRequest(request); token != "header-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestAuthenticateWebSocketRequestRejectsRefreshToken(t *testing.T) {
	t.Parallel()

	manager := jwtx.NewManager("test-secret", "test", time.Minute, time.Hour)
	pair, _, err := manager.IssuePair("user-demo", []string{"owner"})
	if err != nil {
		t.Fatalf("IssuePair returned error: %v", err)
	}

	request := httptest.NewRequest("GET", "/ws?access_token="+pair.RefreshToken, nil)
	if _, _, err := authenticateWebSocketRequest(request, manager); err == nil {
		t.Fatal("expected refresh token rejection")
	}
}

func TestProxyHandlerStripsUserIDQueryAndSetsHeader(t *testing.T) {
	t.Parallel()

	var capturedQuery string
	var capturedUserID string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.Query().Get("user_id")
		capturedUserID = r.Header.Get("X-User-ID")
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("url.Parse returned error: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	service := &service{
		manager:     jwtx.NewManager("test-secret", "test", time.Minute, time.Hour),
		ledgerProxy: proxy,
	}

	pair, _, err := service.manager.IssuePair("user-demo", []string{"owner"})
	if err != nil {
		t.Fatalf("IssuePair returned error: %v", err)
	}

	handler := service.protected(service.proxyHandler(service.ledgerProxy), writeRoles...)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/transactions?user_id=attacker&limit=5", nil)
	request.Header.Set("Authorization", "Bearer "+pair.AccessToken)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	if capturedQuery != "" {
		t.Fatalf("expected stripped user_id query, got %q", capturedQuery)
	}
	if capturedUserID != "user-demo" {
		t.Fatalf("unexpected X-User-ID: %s", capturedUserID)
	}
}

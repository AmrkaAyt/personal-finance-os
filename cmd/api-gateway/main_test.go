package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
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

func TestRegisterWebAppServesEmbeddedAssets(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	registerWebApp(mux)

	indexRecorder := httptest.NewRecorder()
	mux.ServeHTTP(indexRecorder, httptest.NewRequest(http.MethodGet, "/app/", nil))
	if indexRecorder.Code != http.StatusOK {
		t.Fatalf("index status = %d, want %d", indexRecorder.Code, http.StatusOK)
	}
	if !strings.Contains(indexRecorder.Body.String(), "Personal Finance OS") {
		t.Fatal("index response does not contain app shell marker")
	}

	assetRecorder := httptest.NewRecorder()
	mux.ServeHTTP(assetRecorder, httptest.NewRequest(http.MethodGet, "/app/app.js", nil))
	if assetRecorder.Code != http.StatusOK {
		t.Fatalf("asset status = %d, want %d", assetRecorder.Code, http.StatusOK)
	}
	body, err := io.ReadAll(assetRecorder.Result().Body)
	if err != nil {
		t.Fatalf("read asset body: %v", err)
	}
	if !strings.Contains(string(body), "connectWebSocket") {
		t.Fatal("app.js response does not contain realtime client marker")
	}
}

func TestRegisterWebAppRootRedirectAndUnknownNotFound(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	registerWebApp(mux)

	rootRecorder := httptest.NewRecorder()
	mux.ServeHTTP(rootRecorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if rootRecorder.Code != http.StatusTemporaryRedirect {
		t.Fatalf("root status = %d, want %d", rootRecorder.Code, http.StatusTemporaryRedirect)
	}
	if location := rootRecorder.Header().Get("Location"); location != "/app/" {
		t.Fatalf("root redirect location = %q, want /app/", location)
	}

	missingRecorder := httptest.NewRecorder()
	mux.ServeHTTP(missingRecorder, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if missingRecorder.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, want %d", missingRecorder.Code, http.StatusNotFound)
	}
}

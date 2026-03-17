package jwtx

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMiddlewareRejectsRefreshToken(t *testing.T) {
	t.Parallel()

	manager := NewManager("test-secret", "test", time.Minute, time.Hour)
	pair, _, err := manager.IssuePair("user-demo", []string{"owner"})
	if err != nil {
		t.Fatalf("IssuePair returned error: %v", err)
	}

	handler := Middleware(manager, true)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Authorization", "Bearer "+pair.RefreshToken)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", response.Code)
	}
}

func TestMiddlewareAcceptsAccessToken(t *testing.T) {
	t.Parallel()

	manager := NewManager("test-secret", "test", time.Minute, time.Hour)
	pair, _, err := manager.IssuePair("user-demo", []string{"owner"})
	if err != nil {
		t.Fatalf("IssuePair returned error: %v", err)
	}

	handler := Middleware(manager, true)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Authorization", "Bearer "+pair.AccessToken)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", response.Code)
	}
}

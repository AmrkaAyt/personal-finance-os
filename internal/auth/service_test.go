package auth

import (
	"context"
	"testing"
	"time"

	"personal-finance-os/internal/platform/jwtx"
)

func TestLoginAndRefresh(t *testing.T) {
	tokens := jwtx.NewManager("test-secret", "tests", 15*time.Minute, time.Hour)
	service := NewService(tokens, NewMemorySessionStore(), DefaultUsers())

	pair, err := service.Login(context.Background(), "demo", "demo")
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatal("expected access and refresh tokens to be issued")
	}

	refreshed, err := service.Refresh(context.Background(), pair.RefreshToken)
	if err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if refreshed.AccessToken == pair.AccessToken {
		t.Fatal("expected a new access token after refresh")
	}
}

package userctx

import (
	"net/http/httptest"
	"testing"
)

func TestRequireAuthenticatedUserIDRejectsMissingHeader(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest("GET", "/api/v1/transactions", nil)
	if _, err := RequireAuthenticatedUserID(request); err == nil {
		t.Fatal("expected missing header rejection")
	}
}

func TestRequireAuthenticatedUserIDRejectsQueryOverride(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest("GET", "/api/v1/transactions?user_id=attacker", nil)
	request.Header.Set(HeaderUserID, "user-demo")

	if _, err := RequireAuthenticatedUserID(request); err == nil {
		t.Fatal("expected query override rejection")
	}
}

func TestRequireAuthenticatedUserIDAcceptsHeader(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest("GET", "/api/v1/transactions", nil)
	request.Header.Set(HeaderUserID, "user-demo")

	userID, err := RequireAuthenticatedUserID(request)
	if err != nil {
		t.Fatalf("RequireAuthenticatedUserID returned error: %v", err)
	}
	if userID != "user-demo" {
		t.Fatalf("unexpected userID: %s", userID)
	}
}

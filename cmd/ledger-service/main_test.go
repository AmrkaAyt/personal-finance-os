package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandleListTransactionsRejectsMissingIdentity(t *testing.T) {
	t.Parallel()

	service := &service{}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/transactions", nil)
	recorder := httptest.NewRecorder()

	service.handleListTransactions(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
}

func TestHandleListTransactionsRejectsQueryOverride(t *testing.T) {
	t.Parallel()

	service := &service{}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/transactions?user_id=attacker", nil)
	request.Header.Set("X-User-ID", "user-demo")
	recorder := httptest.NewRecorder()

	service.handleListTransactions(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
}

func TestHandleCreateTransactionRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	service := &service{defaultAccountID: "manual-default"}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", strings.NewReader(`{"merchant":"Coffee","amount_cents":450,"user_id":"attacker"}`))
	request.Header.Set("X-User-ID", "user-demo")
	request.Header.Set("Idempotency-Key", "manual-test-key")
	recorder := httptest.NewRecorder()

	service.handleCreateTransaction(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
}

func TestLedgerManualTransactionFromRequestUsesIdempotencyKey(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC)
	input := createTransactionRequest{
		Merchant:    "Coffee Shop",
		Category:    "food",
		AmountCents: -450,
		Currency:    "usd",
		OccurredAt:  &occurredAt,
	}

	first := ledgerManualTransactionFromRequest("user-demo", "default-account", "same-key", input)
	second := ledgerManualTransactionFromRequest("user-demo", "default-account", "same-key", input)

	if first.UserID != "user-demo" {
		t.Fatalf("unexpected userID: %s", first.UserID)
	}
	if first.SourceImportID != "manual:same-key" {
		t.Fatalf("unexpected source import id: %s", first.SourceImportID)
	}
	if first.ID != second.ID || first.Fingerprint != second.Fingerprint {
		t.Fatal("expected deterministic idempotent transaction identity")
	}
}

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"personal-finance-os/internal/insights"
)

type fakeInsightActionStore struct {
	stored insights.Action
	items  []insights.Action
}

func (s *fakeInsightActionStore) Upsert(_ context.Context, action insights.Action) (insights.Action, error) {
	s.stored = action
	return action, nil
}

func (s *fakeInsightActionStore) List(_ context.Context, _ string, _ int) ([]insights.Action, error) {
	return s.items, nil
}

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

func TestHandleCreateInsightActionStoresAuthenticatedUserAction(t *testing.T) {
	t.Parallel()

	store := &fakeInsightActionStore{}
	service := &service{insightActions: store, requestTimeout: time.Second}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/insights/actions", strings.NewReader(`{
		"insight_id":"alert:2026-04-28:large_transaction:warning",
		"insight_type":"alert",
		"action":"acknowledge",
		"metadata":{"alert_type":"large_transaction"}
	}`))
	request.Header.Set("X-User-ID", "user-demo")
	recorder := httptest.NewRecorder()

	service.handleCreateInsightAction(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	if store.stored.UserID != "user-demo" {
		t.Fatalf("unexpected stored user id: %s", store.stored.UserID)
	}
	if store.stored.Action != insights.ActionAcknowledge {
		t.Fatalf("unexpected stored action: %s", store.stored.Action)
	}
}

func TestHandleCreateInsightActionRejectsInvalidSnooze(t *testing.T) {
	t.Parallel()

	service := &service{insightActions: &fakeInsightActionStore{}, requestTimeout: time.Second}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/insights/actions", strings.NewReader(`{
		"insight_id":"alert:large",
		"insight_type":"alert",
		"action":"snooze"
	}`))
	request.Header.Set("X-User-ID", "user-demo")
	recorder := httptest.NewRecorder()

	service.handleCreateInsightAction(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
}

func TestHandleListInsightActionsReturnsUserActions(t *testing.T) {
	t.Parallel()

	service := &service{
		insightActions: &fakeInsightActionStore{items: []insights.Action{{
			ID:          "insact-1",
			UserID:      "user-demo",
			InsightID:   "recurring:rent",
			InsightType: "recurring",
			Action:      insights.ActionConfirmRecurring,
			Metadata:    map[string]string{},
		}}},
		requestTimeout: time.Second,
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/insights/actions", nil)
	request.Header.Set("X-User-ID", "user-demo")
	recorder := httptest.NewRecorder()

	service.handleListInsightActions(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	var response struct {
		Actions []insights.Action `json:"actions"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Actions) != 1 || response.Actions[0].Action != insights.ActionConfirmRecurring {
		t.Fatalf("unexpected response: %+v", response)
	}
}

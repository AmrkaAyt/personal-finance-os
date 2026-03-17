package ledger

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewTransactionOutboxEventIsDeterministic(t *testing.T) {
	t.Parallel()

	transaction := Transaction{
		ID:             "txn-123",
		UserID:         "user-demo",
		AccountID:      "checking",
		SourceImportID: "import-1",
		Fingerprint:    "fp-1",
		Merchant:       "netflix",
		Category:       "subscriptions",
		AmountCents:    1599,
		Currency:       "USD",
		OccurredAt:     time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC),
	}

	first, err := NewTransactionOutboxEvent("transaction.upserted", transaction)
	if err != nil {
		t.Fatalf("first outbox event: %v", err)
	}
	second, err := NewTransactionOutboxEvent("transaction.upserted", transaction)
	if err != nil {
		t.Fatalf("second outbox event: %v", err)
	}

	if first.ID != second.ID {
		t.Fatalf("expected deterministic outbox event id, got %q and %q", first.ID, second.ID)
	}
	if first.MessageKey != "txn-123" {
		t.Fatalf("unexpected message key: %q", first.MessageKey)
	}
	if first.EventType != TransactionUpsertedEventType {
		t.Fatalf("unexpected event type: %q", first.EventType)
	}

	var payload TransactionUpsertedEvent
	if err := json.Unmarshal(first.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.TransactionID != transaction.ID {
		t.Fatalf("unexpected payload transaction id: %q", payload.TransactionID)
	}
	if payload.AmountCents != transaction.AmountCents {
		t.Fatalf("unexpected payload amount: %d", payload.AmountCents)
	}
}

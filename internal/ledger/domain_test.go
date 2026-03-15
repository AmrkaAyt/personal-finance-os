package ledger

import (
	"testing"
	"time"

	parserdomain "personal-finance-os/internal/parser"
)

func TestDetectRecurring(t *testing.T) {
	transactions := []Transaction{
		{Merchant: "Netflix", Category: "subscriptions", Currency: "USD", AmountCents: 1599, OccurredAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)},
		{Merchant: "Netflix", Category: "subscriptions", Currency: "USD", AmountCents: 1599, OccurredAt: time.Date(2026, 2, 4, 0, 0, 0, 0, time.UTC)},
		{Merchant: "Netflix", Category: "subscriptions", Currency: "USD", AmountCents: 1599, OccurredAt: time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)},
		{Merchant: "Coffee", Category: "food", Currency: "USD", AmountCents: 450, OccurredAt: time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC)},
	}

	patterns := DetectRecurring(transactions)
	if len(patterns) != 1 {
		t.Fatalf("expected 1 recurring pattern, got %d", len(patterns))
	}
	if patterns[0].Merchant != "Netflix" {
		t.Fatalf("expected Netflix recurring pattern, got %s", patterns[0].Merchant)
	}
}

func TestNewTransactionFromParsedIsStable(t *testing.T) {
	occurredAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	parsed := parserdomain.Transaction{
		Merchant:    "netflix",
		Category:    "subscriptions",
		Currency:    "usd",
		AmountCents: 1599,
		OccurredAt:  &occurredAt,
		RawLine:     "Netflix,15.99,USD,2026-03-01,subscriptions",
	}

	first := NewTransactionFromParsed("user-demo", "checking", "import-1", parsed)
	second := NewTransactionFromParsed("user-demo", "checking", "import-1", parsed)

	if first.Fingerprint == "" {
		t.Fatal("expected fingerprint to be set")
	}
	if first.Fingerprint != second.Fingerprint {
		t.Fatalf("expected stable fingerprint, got %s and %s", first.Fingerprint, second.Fingerprint)
	}
	if first.ID != second.ID {
		t.Fatalf("expected stable transaction id, got %s and %s", first.ID, second.ID)
	}
	if first.Category != "subscriptions" {
		t.Fatalf("expected normalized category, got %s", first.Category)
	}
	if first.Currency != "USD" {
		t.Fatalf("expected normalized currency, got %s", first.Currency)
	}
}

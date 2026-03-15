package ledger

import (
	"testing"
	"time"
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

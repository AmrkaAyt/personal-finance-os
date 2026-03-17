package rules

import (
	"context"
	"testing"
	"time"

	"personal-finance-os/internal/ledger"
)

func TestEngineLargeTransactionAndBudgetWarning(t *testing.T) {
	engine := NewEngine(Config{
		LargeTransactionThresholdCents: 1500,
		NewMerchantThresholdCents:      1200,
		BudgetWarningRatio:             0.8,
		BudgetCriticalRatio:            1.0,
		BudgetLimitsCents: map[string]int64{
			"subscriptions": 1900,
		},
	}, NewMemoryStore())

	alerts, err := engine.Evaluate(context.Background(), ledger.TransactionUpsertedEvent{
		TransactionID:  "txn-1",
		UserID:         "user-demo",
		SourceImportID: "manual",
		Merchant:       "Netflix",
		Category:       "subscriptions",
		AmountCents:    1599,
		Currency:       "USD",
		OccurredAt:     time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alerts) != 3 {
		t.Fatalf("expected 3 alerts, got %d", len(alerts))
	}
}

func TestEngineSuppressesImportedMerchantAndLargeTransactionAlertsByDefault(t *testing.T) {
	engine := NewEngine(Config{
		LargeTransactionThresholdCents: 1500,
		NewMerchantThresholdCents:      1200,
	}, NewMemoryStore())

	alerts, err := engine.Evaluate(context.Background(), ledger.TransactionUpsertedEvent{
		TransactionID:  "txn-imported-1",
		UserID:         "user-demo",
		SourceImportID: "import-1",
		Merchant:       "Unknown Store",
		Category:       "food",
		AmountCents:    -2000,
		Currency:       "USD",
		OccurredAt:     time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(alerts) != 0 {
		t.Fatalf("expected no imported spam alerts, got %#v", alerts)
	}
}

func TestEngineDeduplicatesBudgetThresholds(t *testing.T) {
	engine := NewEngine(Config{
		BudgetWarningRatio:  0.8,
		BudgetCriticalRatio: 1.0,
		BudgetLimitsCents: map[string]int64{
			"food": 500,
		},
	}, NewMemoryStore())

	event := ledger.TransactionUpsertedEvent{
		TransactionID:  "txn-food-1",
		UserID:         "user-demo",
		SourceImportID: "import-food",
		Merchant:       "Coffee Shop",
		Category:       "food",
		AmountCents:    -450,
		Currency:       "USD",
		OccurredAt:     time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC),
	}

	first, err := engine.Evaluate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(first) != 1 || first[0].Type != AlertTypeBudgetWarning {
		t.Fatalf("expected one budget warning, got %#v", first)
	}

	second, err := engine.Evaluate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error on second evaluation: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("expected no duplicate threshold alert, got %#v", second)
	}
}

func TestEngineMarksNewMerchantOnlyOnce(t *testing.T) {
	engine := NewEngine(Config{
		NewMerchantThresholdCents:  100,
		NotifyImportedNewMerchants: true,
	}, NewMemoryStore())

	event := ledger.TransactionUpsertedEvent{
		TransactionID:  "txn-merchant",
		UserID:         "user-demo",
		SourceImportID: "import-merchant",
		Merchant:       "Unknown Store",
		Category:       "food",
		AmountCents:    250,
		Currency:       "USD",
		OccurredAt:     time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC),
	}

	first, err := engine.Evaluate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(first) != 1 || first[0].Type != AlertTypeNewMerchant {
		t.Fatalf("expected new merchant alert, got %#v", first)
	}

	second, err := engine.Evaluate(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error on second evaluation: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("expected no duplicate merchant alert, got %#v", second)
	}
}

func TestNormalizeExpenseAmountIgnoresTransfers(t *testing.T) {
	got := normalizeExpenseAmount(ledger.TransactionUpsertedEvent{
		Category:    "transfers",
		AmountCents: -250000,
	})
	if got != 0 {
		t.Fatalf("normalizeExpenseAmount() = %d, want 0", got)
	}
}

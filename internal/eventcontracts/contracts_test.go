package eventcontracts_test

import (
	"testing"
	"time"

	"personal-finance-os/internal/eventcontracts"
	"personal-finance-os/internal/imports"
	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/rules"

	"github.com/segmentio/kafka-go"
)

func TestAllContractsAreUniqueAndVersioned(t *testing.T) {
	seen := map[string]struct{}{}
	for _, contract := range eventcontracts.All() {
		if contract.Topic == "" || contract.Type == "" || contract.Version == "" || contract.Producer == "" {
			t.Fatalf("contract has missing identity fields: %#v", contract)
		}
		key := contract.Topic + "|" + contract.Type + "|" + contract.Version
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate contract identity %q", key)
		}
		seen[key] = struct{}{}
	}
}

func TestKnownEventPayloadsValidate(t *testing.T) {
	now := time.Date(2026, 4, 26, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		contract eventcontracts.Contract
		payload  any
	}{
		{
			name:     "statement uploaded",
			contract: eventcontracts.StatementUploaded,
			payload: imports.StatementUploadedEvent{
				UserID:     "user-demo",
				ImportID:   "import-1",
				Filename:   "statement.csv",
				SHA256:     "abc",
				SizeBytes:  10,
				Status:     "queued",
				ReceivedAt: now,
			},
		},
		{
			name:     "statement parsed",
			contract: eventcontracts.StatementParsed,
			payload: imports.StatementParsedEvent{
				UserID:           "user-demo",
				ImportID:         "import-1",
				Filename:         "statement.csv",
				Status:           "parsed",
				Format:           "csv",
				TransactionCount: 3,
				ParsedAt:         now,
			},
		},
		{
			name:     "transaction upserted",
			contract: eventcontracts.TransactionUpserted,
			payload: ledger.TransactionUpsertedEvent{
				TransactionID:   "txn-1",
				UserID:          "user-demo",
				AccountID:       "account-1",
				SourceImportID:  "manual:idempotency",
				Merchant:        "coffee",
				Category:        "food",
				AmountCents:     -450,
				Currency:        "USD",
				OccurredAt:      now,
				TransactionHash: "hash",
			},
		},
		{
			name:     "alert created",
			contract: eventcontracts.AlertCreated,
			payload: rules.Alert{
				ID:        "alert-1",
				UserID:    "user-demo",
				Type:      rules.AlertTypeLargeTransaction,
				Severity:  "warning",
				Message:   "Large transaction detected",
				CreatedAt: now,
			},
		},
		{
			name:     "event quarantine",
			contract: eventcontracts.EventQuarantine,
			payload: kafkax.NewQuarantineEvent("consumer", "group", kafka.Message{
				Topic:     "transaction.upserted",
				Partition: 0,
				Offset:    1,
				Key:       []byte("bad"),
				Value:     []byte(`{`),
			}, kafkax.Permanent(assertionError("bad json")), true),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := eventcontracts.ValidatePayload(tc.contract, tc.payload); err != nil {
				t.Fatalf("ValidatePayload returned error: %v", err)
			}
		})
	}
}

func TestValidatePayloadRejectsMissingRequiredField(t *testing.T) {
	err := eventcontracts.ValidatePayload(eventcontracts.AlertCreated, map[string]any{
		"id":         "alert-1",
		"user_id":    "user-demo",
		"type":       "large_transaction",
		"severity":   "warning",
		"created_at": time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected missing message field to be rejected")
	}
}

type assertionError string

func (e assertionError) Error() string {
	return string(e)
}

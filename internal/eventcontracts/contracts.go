package eventcontracts

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	VersionV1 = "v1"
)

type Contract struct {
	Topic          string
	Type           string
	Version        string
	Producer       string
	Consumers      []string
	RequiredFields []string
	Description    string
}

var (
	StatementUploaded = Contract{
		Topic:    "statement.uploaded",
		Type:     "statement.uploaded",
		Version:  VersionV1,
		Producer: "ingest-service",
		Consumers: []string{
			"audit/observability",
		},
		RequiredFields: []string{"user_id", "import_id", "filename", "sha256", "size_bytes", "status", "received_at"},
		Description:    "Raw statement import was accepted and parse work was queued.",
	}
	StatementParsed = Contract{
		Topic:    "statement.parsed",
		Type:     "statement.parsed",
		Version:  VersionV1,
		Producer: "parser-service",
		Consumers: []string{
			"ledger-service",
		},
		RequiredFields: []string{"user_id", "import_id", "filename", "status", "format", "transaction_count", "parsed_at"},
		Description:    "Parsed statement projection is available for ledger ingestion.",
	}
	TransactionUpserted = Contract{
		Topic:    "transaction.upserted",
		Type:     "transaction.upserted",
		Version:  VersionV1,
		Producer: "ledger-service",
		Consumers: []string{
			"rule-engine",
			"analytics-writer",
			"realtime-gateway",
		},
		RequiredFields: []string{"transaction_id", "user_id", "account_id", "source_import_id", "merchant", "category", "amount_cents", "currency", "occurred_at", "transaction_hash"},
		Description:    "Canonical ledger transaction was inserted or updated.",
	}
	AlertCreated = Contract{
		Topic:    "alert.created",
		Type:     "alert.created",
		Version:  VersionV1,
		Producer: "rule-engine",
		Consumers: []string{
			"analytics-writer",
			"realtime-gateway",
		},
		RequiredFields: []string{"id", "user_id", "type", "severity", "message", "created_at"},
		Description:    "Rule engine emitted a user-facing alert.",
	}
	EventQuarantine = Contract{
		Topic:    "event.quarantine",
		Type:     "event.quarantine",
		Version:  VersionV1,
		Producer: "kafka-consumer-runtime",
		Consumers: []string{
			"quarantine-operator",
		},
		RequiredFields: []string{"id", "service", "source_topic", "source_partition", "source_offset", "error_kind", "error", "payload_sha256", "payload_size", "quarantined_at"},
		Description:    "Kafka consumer isolated a permanently malformed or non-processable event.",
	}
)

func All() []Contract {
	return []Contract{
		StatementUploaded,
		StatementParsed,
		TransactionUpserted,
		AlertCreated,
		EventQuarantine,
	}
}

func ByType(eventType string) (Contract, bool) {
	normalized := strings.TrimSpace(eventType)
	for _, contract := range All() {
		if contract.Type == normalized {
			return contract, true
		}
	}
	return Contract{}, false
}

func ValidatePayload(contract Contract, payload any) error {
	if strings.TrimSpace(contract.Type) == "" {
		return fmt.Errorf("event contract type is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("%s payload marshal: %w", contract.Type, err)
	}
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		return fmt.Errorf("%s payload object decode: %w", contract.Type, err)
	}
	for _, field := range contract.RequiredFields {
		value, ok := object[field]
		if !ok || requiredValueMissing(value) {
			return fmt.Errorf("%s payload missing required field %q", contract.Type, field)
		}
	}
	return nil
}

func requiredValueMissing(value any) bool {
	if value == nil {
		return true
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text) == ""
	}
	return false
}

package ledger

import (
	"encoding/json"
	"strings"
	"time"
)

const TransactionUpsertedEventType = "transaction.upserted"

type OutboxEvent struct {
	ID              string
	Topic           string
	MessageKey      string
	EventType       string
	Payload         json.RawMessage
	PublishAttempts int
	LastError       string
	ClaimOwner      string
	ClaimedAt       *time.Time
	ClaimExpiresAt  *time.Time
	CreatedAt       time.Time
	PublishedAt     *time.Time
}

func TransactionOutboxEventID(transaction Transaction) string {
	return "outbox:" + TransactionUpsertedEventType + ":" + strings.TrimSpace(transaction.ID)
}

func NewTransactionOutboxEvent(topic string, transaction Transaction) (OutboxEvent, error) {
	event := NewTransactionUpsertedEvent(transaction)
	payload, err := json.Marshal(event)
	if err != nil {
		return OutboxEvent{}, err
	}

	return OutboxEvent{
		ID:         TransactionOutboxEventID(transaction),
		Topic:      strings.TrimSpace(topic),
		MessageKey: strings.TrimSpace(transaction.ID),
		EventType:  TransactionUpsertedEventType,
		Payload:    payload,
	}, nil
}

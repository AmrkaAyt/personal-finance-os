package main

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"personal-finance-os/internal/platform/kafkax"
)

func TestMatchesFilter(t *testing.T) {
	event := kafkax.QuarantineEvent{
		ID:          "id-1",
		Service:     "ledger-parsed-consumer",
		SourceTopic: "statement.parsed",
		ErrorKind:   "permanent",
	}

	if !matchesFilter(event, quarantineFilter{Service: "ledger-parsed-consumer"}) {
		t.Fatal("expected service filter to match")
	}
	if matchesFilter(event, quarantineFilter{SourceTopic: "transaction.upserted"}) {
		t.Fatal("expected source topic filter to reject")
	}
}

func TestBuildReplayMessage(t *testing.T) {
	event := kafkax.QuarantineEvent{
		ID:          "id-1",
		SourceTopic: "statement.parsed",
		MessageKey:  "key-1",
		PayloadB64:  base64.StdEncoding.EncodeToString([]byte(`{"ok":true}`)),
	}

	message, topic, err := buildReplayMessage(event, "", replayAudit{
		Operator:   "operator-1",
		Reason:     "fixed schema",
		ApprovedAt: time.Date(2026, 4, 26, 8, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("buildReplayMessage returned error: %v", err)
	}
	if topic != "statement.parsed" {
		t.Fatalf("expected topic statement.parsed, got %q", topic)
	}
	if message.Topic != "statement.parsed" {
		t.Fatalf("expected message topic statement.parsed, got %q", message.Topic)
	}
	if string(message.Key) != "key-1" {
		t.Fatalf("expected key key-1, got %q", string(message.Key))
	}
	if string(message.Value) != `{"ok":true}` {
		t.Fatalf("unexpected payload %q", string(message.Value))
	}
	if len(message.Headers) == 0 {
		t.Fatal("expected replay headers")
	}
	if !hasHeader(message.Headers, "x-quarantine-replay-reason") {
		t.Fatal("expected replay reason header")
	}
}

func TestBuildReplayMessageRejectsMissingPayload(t *testing.T) {
	_, _, err := buildReplayMessage(kafkax.QuarantineEvent{ID: "id-1", SourceTopic: "x"}, "", replayAudit{})
	if err == nil {
		t.Fatal("expected error for missing payload")
	}
}

func TestReplayCommitRequiresApproval(t *testing.T) {
	service := &service{dryRun: false}
	if err := service.validateReplayApproval(1); err == nil {
		t.Fatal("expected approval error")
	}

	service.replayApproved = true
	service.replayReason = "fixed bad producer"
	service.replayOperator = "operator-1"
	if err := service.validateReplayApproval(1); err != nil {
		t.Fatalf("validateReplayApproval returned error: %v", err)
	}
}

func hasHeader(headers []kafka.Header, key string) bool {
	for _, header := range headers {
		if header.Key == key {
			return true
		}
	}
	return false
}

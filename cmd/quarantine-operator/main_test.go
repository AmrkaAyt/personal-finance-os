package main

import (
	"encoding/base64"
	"testing"

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

	message, topic, err := buildReplayMessage(event, "")
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
}

func TestBuildReplayMessageRejectsMissingPayload(t *testing.T) {
	_, _, err := buildReplayMessage(kafkax.QuarantineEvent{ID: "id-1", SourceTopic: "x"}, "")
	if err == nil {
		t.Fatal("expected error for missing payload")
	}
}

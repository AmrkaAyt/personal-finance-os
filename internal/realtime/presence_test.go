package realtime

import (
	"testing"

	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/platform/ws"
)

func TestTransactionEnvelope(t *testing.T) {
	t.Parallel()

	envelope := TransactionEnvelope(ledger.TransactionUpsertedEvent{
		UserID:      "user-demo",
		Category:    "food",
		AmountCents: -450,
	})
	if envelope.Type != "transaction.upserted" {
		t.Fatalf("unexpected type: %s", envelope.Type)
	}
	if envelope.Channel != ChannelTransactions {
		t.Fatalf("unexpected channel: %s", envelope.Channel)
	}
	if envelope.UserID != "user-demo" {
		t.Fatalf("unexpected user_id: %s", envelope.UserID)
	}
}

func TestPresenceEnvelope(t *testing.T) {
	t.Parallel()

	envelope := PresenceEnvelope(3, 2)
	if envelope.Type != "presence.snapshot" {
		t.Fatalf("unexpected type: %s", envelope.Type)
	}
	if envelope.Channel != ChannelDashboard {
		t.Fatalf("unexpected channel: %s", envelope.Channel)
	}
}

func TestRedisStoreKeys(t *testing.T) {
	t.Parallel()

	store := &RedisStore{prefix: "ws"}
	if key := store.presenceKey("user-demo"); key != "ws:presence:user-demo" {
		t.Fatalf("unexpected presence key: %s", key)
	}
	if key := store.subscriptionsKey("conn-1"); key != "ws:subscriptions:conn-1" {
		t.Fatalf("unexpected subscriptions key: %s", key)
	}
}

func TestNormalizePart(t *testing.T) {
	t.Parallel()

	if got := normalizePart(""); got != "unknown" {
		t.Fatalf("unexpected normalized part: %s", got)
	}
}

func TestPresenceStoreCompileShape(t *testing.T) {
	t.Parallel()

	var _ PresenceStore = (*RedisStore)(nil)
	_ = ws.ClientInfo{}
}

package notificationdigest

import (
	"context"
	"testing"
	"time"

	"personal-finance-os/internal/rules"
)

func TestMemoryStoreAppendAndTake(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	now := time.Now().UTC()
	job := rules.NotificationJob{
		Alert: rules.Alert{
			ID:        "alert-1",
			UserID:    "user-1",
			Type:      "new_merchant",
			Severity:  "warning",
			Message:   "alert message",
			CreatedAt: now,
		},
		Channel: "telegram",
		ChatID:  "chat-1",
	}

	digest, err := store.Append(context.Background(), "digest-1", job, now.Add(30*time.Second), 10)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if digest.Counts["new_merchant"] != 1 {
		t.Fatalf("digest count = %d, want 1", digest.Counts["new_merchant"])
	}

	keys, err := store.DueKeys(context.Background(), now.Add(31*time.Second), 10)
	if err != nil {
		t.Fatalf("DueKeys() error = %v", err)
	}
	if len(keys) != 1 || keys[0] != "digest-1" {
		t.Fatalf("DueKeys() = %#v, want [digest-1]", keys)
	}

	stored, ok, err := store.Take(context.Background(), "digest-1")
	if err != nil {
		t.Fatalf("Take() error = %v", err)
	}
	if !ok {
		t.Fatal("Take() ok = false, want true")
	}
	if len(stored.Items) != 1 {
		t.Fatalf("len(stored.Items) = %d, want 1", len(stored.Items))
	}
}

package telegramauth

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreBindingLifecycle(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	binding := Binding{
		ChatID:   "chat-1",
		UserID:   "user-1",
		Username: "user-1",
		Roles:    []string{"owner"},
		BoundAt:  time.Now().UTC(),
	}

	if err := store.Save(context.Background(), binding); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	stored, ok, err := store.Get(context.Background(), "chat-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	if stored.UserID != binding.UserID {
		t.Fatalf("stored.UserID = %q, want %q", stored.UserID, binding.UserID)
	}

	if err := store.Delete(context.Background(), "chat-1"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, ok, _ := store.Get(context.Background(), "chat-1"); ok {
		t.Fatal("binding still exists after Delete")
	}
}

func TestMemoryStorePendingLinkConsumeOnce(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	pending := PendingLink{
		Code:      "ABC12345",
		ChatID:    "chat-42",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
	}

	if err := store.SavePending(context.Background(), pending); err != nil {
		t.Fatalf("SavePending() error = %v", err)
	}

	consumed, ok, err := store.ConsumePending(context.Background(), "abc12345")
	if err != nil {
		t.Fatalf("ConsumePending() error = %v", err)
	}
	if !ok {
		t.Fatal("ConsumePending() ok = false, want true")
	}
	if consumed.ChatID != pending.ChatID {
		t.Fatalf("consumed.ChatID = %q, want %q", consumed.ChatID, pending.ChatID)
	}

	if _, ok, err := store.ConsumePending(context.Background(), "ABC12345"); err != nil {
		t.Fatalf("second ConsumePending() error = %v", err)
	} else if ok {
		t.Fatal("second ConsumePending() ok = true, want false")
	}
}

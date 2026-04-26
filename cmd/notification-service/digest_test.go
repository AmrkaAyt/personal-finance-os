package main

import (
	"strings"
	"testing"
	"time"

	"personal-finance-os/internal/notificationdigest"
	"personal-finance-os/internal/rules"
)

func TestShouldBatch(t *testing.T) {
	svc := &service{
		digestEnabled: true,
		digestWindow:  15 * time.Second,
		defaultChatID: "chat-default",
	}

	job := rules.NewNotificationJob(rules.Alert{
		ID:        "a1",
		UserID:    "user-1",
		Type:      rules.AlertTypeNewMerchant,
		Severity:  "warning",
		Message:   "new merchant",
		CreatedAt: time.Now().UTC(),
	}, "chat-1")
	if !svc.shouldBatch(job) {
		t.Fatal("expected warning alert to be batched")
	}

	job.Alert.Severity = "critical"
	if svc.shouldBatch(job) {
		t.Fatal("expected critical alert to bypass batching")
	}

	job.Alert.Severity = "warning"
	job.Attempt = 1
	if svc.shouldBatch(job) {
		t.Fatal("expected retried alert to bypass batching")
	}

	job.Attempt = 0
	job.Alert.Type = "digest"
	if svc.shouldBatch(job) {
		t.Fatal("expected digest job to bypass batching")
	}
}

func TestBuildTelegramDigestText(t *testing.T) {
	digest := notificationdigest.Digest{
		Key:            "telegram:user-1:import:imp-1",
		UserID:         "user-1",
		ChatID:         "chat-1",
		Channel:        "telegram",
		SourceImportID: "imp-1",
		Counts: map[string]int{
			rules.AlertTypeNewMerchant:      4,
			rules.AlertTypeBudgetWarning:    1,
			rules.AlertTypeLargeTransaction: 2,
		},
		Items: []notificationdigest.Item{
			{Type: rules.AlertTypeNewMerchant, Merchant: "yandex.go", AmountCents: 206000, Severity: "warning"},
			{Type: rules.AlertTypeLargeTransaction, Merchant: "galmart", AmountCents: 55500, Severity: "warning"},
		},
	}

	text := buildTelegramDigestText(digest, 15*time.Second)
	for _, expected := range []string{
		"Сводка алертов",
		"Import ID: imp-1",
		"Новый мерчант: 4",
		"Крупная трата: 2",
		"Мерчанты: yandex.go, galmart",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("expected %q in digest text, got:\n%s", expected, text)
		}
	}
}

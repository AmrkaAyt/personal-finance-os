package main

import (
	"context"
	"testing"
	"time"

	"personal-finance-os/internal/notificationprefs"
	"personal-finance-os/internal/rules"
)

type mockPrefsStore struct {
	prefs notificationprefs.Preferences
}

func (m mockPrefsStore) Get(context.Context, string) (notificationprefs.Preferences, error) {
	return m.prefs, nil
}

func (m mockPrefsStore) Upsert(context.Context, notificationprefs.Preferences) (notificationprefs.Preferences, error) {
	return m.prefs, nil
}

func TestDeliveryDecisionSuppressesDisabledType(t *testing.T) {
	svc := &service{
		digestEnabled: true,
		digestWindow:  15 * time.Second,
		prefsStore: mockPrefsStore{prefs: notificationprefs.Preferences{
			UserID:             "user-1",
			TelegramEnabled:    true,
			BatchNonCritical:   true,
			QuietTimezone:      "UTC",
			DisabledAlertTypes: []string{rules.AlertTypeNewMerchant},
		}},
	}

	mode, _, err := svc.deliveryDecision(context.Background(), rules.NotificationJob{
		Alert: rules.Alert{
			UserID:   "user-1",
			Type:     rules.AlertTypeNewMerchant,
			Severity: "warning",
		},
		Channel: "telegram",
	})
	if err != nil {
		t.Fatalf("deliveryDecision returned error: %v", err)
	}
	if mode != deliverySuppress {
		t.Fatalf("expected suppress, got %s", mode)
	}
}

func TestDeliveryDecisionBatchesQuietWindow(t *testing.T) {
	svc := &service{
		digestEnabled: true,
		digestWindow:  15 * time.Second,
		prefsStore: mockPrefsStore{prefs: notificationprefs.Preferences{
			UserID:            "user-1",
			TelegramEnabled:   true,
			BatchNonCritical:  true,
			QuietHoursEnabled: true,
			QuietStartMinute:  0,
			QuietEndMinute:    1439,
			QuietTimezone:     "UTC",
		}},
	}

	mode, dueAt, err := svc.deliveryDecision(context.Background(), rules.NotificationJob{
		Alert: rules.Alert{
			UserID:   "user-1",
			Type:     rules.AlertTypeBudgetWarning,
			Severity: "warning",
		},
		Channel: "telegram",
	})
	if err != nil {
		t.Fatalf("deliveryDecision returned error: %v", err)
	}
	if mode != deliveryBatch {
		t.Fatalf("expected batch, got %s", mode)
	}
	if dueAt.IsZero() {
		t.Fatal("expected dueAt for quiet-window batch")
	}
}

func TestDeliveryDecisionBypassesSystemNotifications(t *testing.T) {
	svc := &service{
		digestEnabled: true,
		digestWindow:  15 * time.Second,
		prefsStore: mockPrefsStore{prefs: notificationprefs.Preferences{
			UserID:            "user-1",
			TelegramEnabled:   false,
			BatchNonCritical:  false,
			QuietHoursEnabled: true,
			QuietStartMinute:  0,
			QuietEndMinute:    1439,
			QuietTimezone:     "UTC",
		}},
	}

	mode, _, err := svc.deliveryDecision(context.Background(), rules.NotificationJob{
		Alert: rules.Alert{
			Type:     "telegram_command",
			Severity: "info",
		},
		Channel: "telegram",
	})
	if err != nil {
		t.Fatalf("deliveryDecision returned error: %v", err)
	}
	if mode != deliveryImmediate {
		t.Fatalf("expected immediate, got %s", mode)
	}
}

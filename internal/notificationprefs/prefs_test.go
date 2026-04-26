package notificationprefs

import (
	"testing"
	"time"
)

func TestQuietWindowCrossMidnight(t *testing.T) {
	prefs := Default("user-1")
	prefs.QuietHoursEnabled = true
	prefs.QuietTimezone = "UTC"
	prefs.QuietStartMinute = 23 * 60
	prefs.QuietEndMinute = 8 * 60

	active, until, err := prefs.QuietWindow(time.Date(2026, 3, 19, 1, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("QuietWindow returned error: %v", err)
	}
	if !active {
		t.Fatal("expected quiet window to be active")
	}
	expected := time.Date(2026, 3, 19, 8, 0, 0, 0, time.UTC)
	if !until.Equal(expected) {
		t.Fatalf("expected quiet window until %s, got %s", expected, until)
	}
}

func TestQuietWindowInactive(t *testing.T) {
	prefs := Default("user-1")
	prefs.QuietHoursEnabled = true
	prefs.QuietTimezone = "UTC"
	prefs.QuietStartMinute = 23 * 60
	prefs.QuietEndMinute = 8 * 60

	active, _, err := prefs.QuietWindow(time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("QuietWindow returned error: %v", err)
	}
	if active {
		t.Fatal("expected quiet window to be inactive")
	}
}

func TestNormalizeDeduplicatesAlertTypes(t *testing.T) {
	normalized, err := Normalize(Preferences{
		UserID:             "user-1",
		TelegramEnabled:    true,
		BatchNonCritical:   true,
		QuietTimezone:      "UTC",
		DisabledAlertTypes: []string{"new_merchant", " new_merchant ", "", "budget_warning"},
	})
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	if len(normalized.DisabledAlertTypes) != 2 {
		t.Fatalf("expected 2 alert types, got %d", len(normalized.DisabledAlertTypes))
	}
}

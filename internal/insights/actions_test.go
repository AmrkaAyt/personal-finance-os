package insights

import (
	"testing"
	"time"
)

func TestNormalizeRequiresSnoozedUntilForSnooze(t *testing.T) {
	t.Parallel()

	_, err := Normalize(Action{
		UserID:      "user-demo",
		InsightID:   "alert:large",
		InsightType: "alert",
		Action:      ActionSnooze,
	})
	if err == nil {
		t.Fatal("expected snooze without snoozed_until to fail")
	}
}

func TestNormalizeBuildsStableIDAndCleansMetadata(t *testing.T) {
	t.Parallel()

	snoozedUntil := time.Date(2026, 4, 29, 10, 0, 0, 0, time.FixedZone("local", 3600))
	action, err := Normalize(Action{
		UserID:       " user-demo ",
		InsightID:    " alert:large ",
		InsightType:  " Alert ",
		Action:       " Snooze ",
		SnoozedUntil: &snoozedUntil,
		Metadata: map[string]string{
			" Alert_Type ": " large_transaction ",
			"":             "ignored",
		},
	})
	if err != nil {
		t.Fatalf("Normalize returned error: %v", err)
	}
	if action.ID != ActionID("user-demo", "alert:large") {
		t.Fatalf("unexpected action id: %s", action.ID)
	}
	if action.UserID != "user-demo" || action.InsightType != "alert" || action.Action != ActionSnooze {
		t.Fatalf("unexpected normalized action: %+v", action)
	}
	if action.Metadata["alert_type"] != "large_transaction" {
		t.Fatalf("metadata was not normalized: %+v", action.Metadata)
	}
	if action.SnoozedUntil.Location() != time.UTC {
		t.Fatalf("expected UTC snoozed_until, got %s", action.SnoozedUntil.Location())
	}
}

func TestNormalizeRejectsUnsupportedAction(t *testing.T) {
	t.Parallel()

	_, err := Normalize(Action{
		UserID:      "user-demo",
		InsightID:   "alert:large",
		InsightType: "alert",
		Action:      "delete",
	})
	if err == nil {
		t.Fatal("expected unsupported action to fail")
	}
}

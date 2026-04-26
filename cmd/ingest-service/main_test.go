package main

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestRawStatusUpdateFilterDoesNotDowngradeTerminalStatuses(t *testing.T) {
	filter := rawStatusUpdateFilter("user-demo", "import-1", "queued")

	statusFilter, ok := filter["status"].(bson.M)
	if !ok {
		t.Fatalf("expected status guard, got %#v", filter["status"])
	}
	blocked, ok := statusFilter["$nin"].([]string)
	if !ok {
		t.Fatalf("expected $nin status guard, got %#v", statusFilter["$nin"])
	}

	for _, status := range []string{"parsing", "parsed", "parsed_pending_event"} {
		if !containsString(blocked, status) {
			t.Fatalf("expected queued update to block %q, got %#v", status, blocked)
		}
	}
}

func TestRawStatusUpdateFilterAllowsTerminalStatusWrite(t *testing.T) {
	filter := rawStatusUpdateFilter("user-demo", "import-1", "parsed")
	if _, ok := filter["status"]; ok {
		t.Fatalf("did not expect status guard for parsed update: %#v", filter)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

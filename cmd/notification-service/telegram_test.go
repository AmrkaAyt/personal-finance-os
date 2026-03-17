package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseTelegramCommand(t *testing.T) {
	t.Parallel()

	command, args := parseTelegramCommand("/report@finance_bot month")
	if command != "/report" {
		t.Fatalf("command = %q, want /report", command)
	}
	if len(args) != 1 || args[0] != "month" {
		t.Fatalf("args = %#v, want [month]", args)
	}
}

func TestReportWindow(t *testing.T) {
	t.Parallel()

	from, to := reportWindow("today")
	if from.Format("2006-01-02") != to.Format("2006-01-02") {
		t.Fatalf("today window mismatch: from=%s to=%s", from, to)
	}

	from, to = reportWindow("month")
	if to.Before(from) {
		t.Fatalf("month window invalid: from=%s to=%s", from, to)
	}
	if from.Day() != 1 {
		t.Fatalf("month start day = %d, want 1", from.Day())
	}
}

func TestParseAllowedChatIDs(t *testing.T) {
	t.Parallel()

	ids := parseAllowedChatIDs("1, 2,3", "")
	if len(ids) != 3 {
		t.Fatalf("len(ids) = %d, want 3", len(ids))
	}

	ids = parseAllowedChatIDs("", "42")
	if len(ids) != 1 {
		t.Fatalf("len(ids) = %d, want 1", len(ids))
	}
	if _, ok := ids["42"]; !ok {
		t.Fatal("fallback chat id not present")
	}
}

func TestTelegramBotStateNextOffset(t *testing.T) {
	t.Parallel()

	var state telegramBotState
	state.setLastUpdateID(100)
	if got := state.nextOffset(); got != 101 {
		t.Fatalf("nextOffset = %d, want 101", got)
	}
	state.setCommand(123, 105, "/help")
	snapshot := state.snapshot()
	if snapshot["last_command"] != "/help" {
		t.Fatalf("last_command = %v, want /help", snapshot["last_command"])
	}
	if snapshot["last_chat_id"] != "123" {
		t.Fatalf("last_chat_id = %v, want 123", snapshot["last_chat_id"])
	}
	if snapshot["last_poll_at"].(time.Time).IsZero() {
		t.Fatal("last_poll_at is zero")
	}
}

func TestIsSupportedTelegramImport(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"statement.csv", "statement.pdf", "statement.txt"} {
		if !isSupportedTelegramImport(name) {
			t.Fatalf("expected %s to be supported", name)
		}
	}
	if isSupportedTelegramImport("statement.exe") {
		t.Fatal("expected .exe to be rejected")
	}
}

func TestBuildTelegramImportAcceptedText(t *testing.T) {
	t.Parallel()

	text := buildTelegramImportAcceptedText(ingestImportResponse{
		ImportID:      "import-1",
		Filename:      "gold_statement.pdf",
		Status:        "queued",
		AlreadyExists: true,
	})
	for _, fragment := range []string{"Выписка принята.", "gold_statement.pdf", "import-1", "Дубликат: да"} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("text %q does not contain %q", text, fragment)
		}
	}
}

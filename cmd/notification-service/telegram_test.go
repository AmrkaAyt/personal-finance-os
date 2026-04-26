package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"personal-finance-os/internal/telegramauth"
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

func TestBuildTelegramLinkInstructionsText(t *testing.T) {
	t.Parallel()

	text := buildTelegramLinkInstructionsText("abc12345", "http://localhost:8080", 10*time.Minute)
	for _, fragment := range []string{"Код привязки Telegram создан.", "ABC12345", "http://localhost:8080/api/v1/notifications/telegram/link/confirm"} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("text %q does not contain %q", text, fragment)
		}
	}
}

func TestHandleTelegramLinkConfirm(t *testing.T) {
	t.Parallel()

	store := telegramauth.NewMemoryStore()
	service := &service{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		authStore:  store,
		linkStore:  store,
		httpClient: http.DefaultClient,
	}

	if err := store.SavePending(context.Background(), telegramauth.PendingLink{
		Code:      "ABC12345",
		ChatID:    "1463353414",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(10 * time.Minute),
	}); err != nil {
		t.Fatalf("SavePending() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/telegram/link/confirm", strings.NewReader(`{"code":"abc12345"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-User-ID", "user-1")
	request.Header.Set("X-User-Roles", "owner,member")

	recorder := httptest.NewRecorder()
	service.handleTelegramLinkConfirm(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response json error = %v", err)
	}
	if payload["status"] != "linked" {
		t.Fatalf("status payload = %v, want linked", payload["status"])
	}

	binding, ok, err := store.Get(context.Background(), "1463353414")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("binding not saved")
	}
	if binding.UserID != "user-1" {
		t.Fatalf("binding.UserID = %q, want user-1", binding.UserID)
	}
	if len(binding.Roles) != 2 {
		t.Fatalf("binding.Roles len = %d, want 2", len(binding.Roles))
	}
}

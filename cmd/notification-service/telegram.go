package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"personal-finance-os/internal/rules"
	"personal-finance-os/internal/telegramauth"
)

const telegramImportMaxBytes = 10 << 20

type telegramGetUpdatesResponse struct {
	OK          bool             `json:"ok"`
	Result      []telegramUpdate `json:"result"`
	Description string           `json:"description"`
}

type telegramUpdate struct {
	UpdateID int64            `json:"update_id"`
	Message  *telegramMessage `json:"message"`
}

type telegramMessage struct {
	MessageID int64             `json:"message_id"`
	Date      int64             `json:"date"`
	Text      string            `json:"text"`
	Caption   string            `json:"caption"`
	Chat      telegramChat      `json:"chat"`
	Document  *telegramDocument `json:"document"`
}

type telegramChat struct {
	ID int64 `json:"id"`
}

type telegramDocument struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	FileName     string `json:"file_name"`
	MimeType     string `json:"mime_type"`
	FileSize     int64  `json:"file_size"`
}

type telegramGetFileResponse struct {
	OK          bool         `json:"ok"`
	Result      telegramFile `json:"result"`
	Description string       `json:"description"`
}

type telegramFile struct {
	FileID   string `json:"file_id"`
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size"`
}

type analyticsHTTPResponse struct {
	Result struct {
		Data []map[string]any `json:"data"`
	} `json:"result"`
}

type ledgerTransactionsResponse struct {
	Transactions []ledgerTransaction `json:"transactions"`
}

type ledgerTransaction struct {
	ID          string    `json:"id"`
	Merchant    string    `json:"merchant"`
	Category    string    `json:"category"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	OccurredAt  time.Time `json:"occurred_at"`
}

type ingestImportResponse struct {
	ImportID      string    `json:"import_id"`
	Filename      string    `json:"filename"`
	Status        string    `json:"status"`
	SizeBytes     int       `json:"size_bytes"`
	AlreadyExists bool      `json:"already_exists"`
	ReceivedAt    time.Time `json:"received_at"`
}

type parsedImportResponse struct {
	UserID   string `json:"user_id"`
	ImportID string `json:"import_id"`
	Filename string `json:"filename"`
	Status   string `json:"status"`
	Summary  struct {
		Format           string   `json:"format"`
		TransactionCount int      `json:"transaction_count"`
		Merchants        []string `json:"merchants"`
		TotalDebitCents  int64    `json:"total_debit_cents"`
		TotalCreditCents int64    `json:"total_credit_cents"`
	} `json:"summary"`
}

type authVerifyRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type authVerifyResponse struct {
	UserID   string   `json:"user_id"`
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
}

func (s *service) pollTelegramUpdates(ctx context.Context, logger *slog.Logger) error {
	if strings.TrimSpace(s.telegramToken) == "" || !s.telegramPollingEnabled {
		logger.Info("telegram polling disabled", "token_configured", strings.TrimSpace(s.telegramToken) != "", "polling_enabled", s.telegramPollingEnabled)
		return nil
	}

	logger.Info("telegram polling started", "poll_interval", s.telegramPollInterval.String(), "allowed_chats", len(s.allowedTelegramChatIDs))
	ticker := time.NewTicker(s.telegramPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if _, err := s.pollTelegramOnce(ctx); err != nil {
			s.botState.setError(err.Error())
			logger.Error("telegram polling failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *service) pollTelegramOnce(ctx context.Context) (int, error) {
	if strings.TrimSpace(s.telegramToken) == "" {
		return 0, nil
	}

	s.pollMu.Lock()
	defer s.pollMu.Unlock()

	operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	updates, err := s.fetchTelegramUpdates(operationCtx)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, update := range updates {
		s.botState.setLastUpdateID(update.UpdateID)
		if update.Message == nil {
			continue
		}
		if !s.isAllowedChat(update.Message.Chat.ID) {
			s.logger.Warn("telegram chat rejected", "chat_id", update.Message.Chat.ID)
			continue
		}

		switch {
		case update.Message.Document != nil:
			responseText := s.handleTelegramDocument(operationCtx, update.Message)
			s.botState.setCommand(update.Message.Chat.ID, update.UpdateID, "document")
			if strings.TrimSpace(responseText) != "" {
				if err := s.sendTelegramText(operationCtx, strconv.FormatInt(update.Message.Chat.ID, 10), responseText); err != nil {
					return processed, err
				}
			}
			processed++
		case strings.TrimSpace(update.Message.Text) != "":
			command, responseText := s.handleTelegramCommand(operationCtx, update.Message)
			s.botState.setCommand(update.Message.Chat.ID, update.UpdateID, command)
			if strings.TrimSpace(responseText) != "" {
				if err := s.sendTelegramText(operationCtx, strconv.FormatInt(update.Message.Chat.ID, 10), responseText); err != nil {
					return processed, err
				}
			}
			processed++
		}
	}

	s.botState.touch()
	return processed, nil
}

func (s *service) fetchTelegramUpdates(ctx context.Context) ([]telegramUpdate, error) {
	values := url.Values{}
	values.Set("timeout", "5")
	values.Set("allowed_updates", `["message"]`)
	if offset := s.botState.nextOffset(); offset > 0 {
		values.Set("offset", strconv.FormatInt(offset, 10))
	}

	endpoint := fmt.Sprintf("%s/bot%s/getUpdates?%s", s.telegramAPIBaseURL, s.telegramToken, values.Encode())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	response, err := s.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	var payload telegramGetUpdatesResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if response.StatusCode >= http.StatusMultipleChoices || !payload.OK {
		return nil, fmt.Errorf("telegram getUpdates failed: status=%d description=%s", response.StatusCode, payload.Description)
	}
	return payload.Result, nil
}

func (s *service) handleTelegramCommand(ctx context.Context, message *telegramMessage) (string, string) {
	command, args := parseTelegramCommand(message.Text)

	switch command {
	case "/start", "/help":
		return command, s.telegramHelpText()
	case "/status":
		return command, s.telegramStatusText(ctx, message.Chat.ID)
	case "/login":
		return command, s.telegramLoginText(ctx, message.Chat.ID, args)
	case "/logout":
		return command, s.telegramLogoutText(ctx, message.Chat.ID)
	case "/whoami":
		return command, s.telegramWhoAmIText(ctx, message.Chat.ID)
	case "/report":
		return command, s.telegramReportText(ctx, message.Chat.ID, firstArg(args))
	case "/alerts":
		return command, s.telegramAlertsText(ctx, message.Chat.ID)
	case "/transactions":
		return command, s.telegramTransactionsText(ctx, message.Chat.ID, firstArg(args))
	default:
		return command, "Неизвестная команда.\n\n" + s.telegramHelpText()
	}
}

func (s *service) handleTelegramDocument(ctx context.Context, message *telegramMessage) string {
	document := message.Document
	if document == nil {
		return ""
	}
	binding, ok, err := s.resolveTelegramBinding(ctx, message.Chat.ID)
	if err != nil {
		return "Ошибка проверки авторизации Telegram."
	}
	if !ok {
		return "Нет авторизации. Сначала выполни /login <username> <password>."
	}
	if !isSupportedTelegramImport(document.FileName) {
		return "Неподдерживаемый тип файла. Отправь CSV или PDF с текстовым слоем."
	}
	if document.FileSize > telegramImportMaxBytes {
		return fmt.Sprintf("Файл слишком большой. Лимит: %d МБ.", telegramImportMaxBytes>>20)
	}

	payload, err := s.downloadTelegramDocument(ctx, document.FileID)
	if err != nil {
		return "Не удалось скачать выписку."
	}

	imported, err := s.forwardImportToIngest(ctx, binding.UserID, document.FileName, payload)
	if err != nil {
		return "Не удалось загрузить выписку."
	}

	chatID := strconv.FormatInt(message.Chat.ID, 10)
	s.scheduleTelegramImportSummary(chatID, binding.UserID, imported.ImportID)
	return buildTelegramImportAcceptedText(imported)
}

func (s *service) downloadTelegramDocument(ctx context.Context, fileID string) ([]byte, error) {
	fileMeta, err := s.fetchTelegramFile(ctx, fileID)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s/file/bot%s/%s", s.telegramAPIBaseURL, s.telegramToken, strings.TrimLeft(fileMeta.FilePath, "/"))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	response, err := s.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return nil, fmt.Errorf("telegram file download failed: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	payload, err := io.ReadAll(io.LimitReader(response.Body, telegramImportMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > telegramImportMaxBytes {
		return nil, fmt.Errorf("file exceeds %d bytes", telegramImportMaxBytes)
	}
	return payload, nil
}

func (s *service) fetchTelegramFile(ctx context.Context, fileID string) (telegramFile, error) {
	endpoint := fmt.Sprintf("%s/bot%s/getFile?file_id=%s", s.telegramAPIBaseURL, s.telegramToken, url.QueryEscape(fileID))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return telegramFile{}, err
	}

	response, err := s.httpClient.Do(request)
	if err != nil {
		return telegramFile{}, err
	}
	defer response.Body.Close()

	var payload telegramGetFileResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return telegramFile{}, err
	}
	if response.StatusCode >= http.StatusMultipleChoices || !payload.OK {
		return telegramFile{}, fmt.Errorf("telegram getFile failed: status=%d description=%s", response.StatusCode, payload.Description)
	}
	return payload.Result, nil
}

func (s *service) forwardImportToIngest(ctx context.Context, userID, filename string, payload []byte) (ingestImportResponse, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return ingestImportResponse{}, err
	}
	if _, err := part.Write(payload); err != nil {
		return ingestImportResponse{}, err
	}
	if err := writer.Close(); err != nil {
		return ingestImportResponse{}, err
	}

	endpoint := s.ingestServiceURL + "/imports/raw"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return ingestImportResponse{}, err
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("X-User-ID", strings.TrimSpace(userID))

	response, err := s.httpClient.Do(request)
	if err != nil {
		return ingestImportResponse{}, err
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return ingestImportResponse{}, fmt.Errorf("ingest status=%d body=%s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	var imported ingestImportResponse
	err = json.NewDecoder(response.Body).Decode(&imported)
	return imported, err
}

func (s *service) scheduleTelegramImportSummary(chatID, userID, importID string) {
	if strings.TrimSpace(chatID) == "" || strings.TrimSpace(userID) == "" || strings.TrimSpace(importID) == "" || strings.TrimSpace(s.parserServiceURL) == "" {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		parsed, ok := s.waitForParsedImport(ctx, userID, importID)
		if !ok {
			return
		}
		if err := s.sendTelegramText(ctx, chatID, buildTelegramImportSummaryText(parsed)); err != nil {
			s.logger.Error("failed to send telegram import summary", "import_id", importID, "error", err)
		}
	}()
}

func (s *service) waitForParsedImport(ctx context.Context, userID, importID string) (parsedImportResponse, bool) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		parsed, found, err := s.fetchParsedImport(ctx, userID, importID)
		if err == nil && found {
			return parsed, true
		}

		select {
		case <-ctx.Done():
			return parsedImportResponse{}, false
		case <-ticker.C:
		}
	}
}

func (s *service) fetchParsedImport(ctx context.Context, userID, importID string) (parsedImportResponse, bool, error) {
	endpoint := fmt.Sprintf("%s/parser/results/%s", s.parserServiceURL, url.PathEscape(importID))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return parsedImportResponse{}, false, err
	}
	if strings.TrimSpace(userID) != "" {
		request.Header.Set("X-User-ID", strings.TrimSpace(userID))
	}

	response, err := s.httpClient.Do(request)
	if err != nil {
		return parsedImportResponse{}, false, err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotFound {
		return parsedImportResponse{}, false, nil
	}
	if response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return parsedImportResponse{}, false, fmt.Errorf("parser status=%d body=%s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed parsedImportResponse
	if err := json.NewDecoder(response.Body).Decode(&parsed); err != nil {
		return parsedImportResponse{}, false, err
	}
	return parsed, true, nil
}

func (s *service) telegramHelpText() string {
	return strings.Join([]string{
		"Доступные команды:",
		"/help",
		"/status",
		"/login <username> <password>",
		"/logout",
		"/whoami",
		"/report [today|month]",
		"/alerts",
		"/transactions [limit]",
		"",
		"Также можно отправить CSV или PDF-выписку как документ.",
	}, "\n")
}

func (s *service) telegramStatusText(ctx context.Context, chatID int64) string {
	state := s.botState.snapshot()
	binding, ok, err := s.resolveTelegramBinding(ctx, chatID)
	authLine := "Авторизация: не привязана"
	if err != nil {
		authLine = "Авторизация: ошибка проверки"
	} else if ok {
		authLine = fmt.Sprintf("Авторизация: %s (%s)", binding.Username, binding.UserID)
	}
	return fmt.Sprintf(
		"Сервис уведомлений работает.\nPolling: %t\nОчередь: %s\nDLQ: %s\n%s\nПоследняя команда: %s\nПоследний чат: %s",
		s.telegramPollingEnabled,
		s.queue,
		s.dlq,
		authLine,
		firstNonEmpty(state["last_command"].(string), "n/a"),
		firstNonEmpty(state["last_chat_id"].(string), "n/a"),
	)
}

func (s *service) telegramLoginText(ctx context.Context, chatID int64, args []string) string {
	if len(args) < 2 {
		return "Использование: /login <username> <password>"
	}
	binding, err := s.verifyTelegramCredentials(ctx, strings.TrimSpace(args[0]), strings.TrimSpace(args[1]))
	if err != nil {
		return "Не удалось выполнить вход."
	}
	binding.ChatID = strconv.FormatInt(chatID, 10)
	binding.BoundAt = time.Now().UTC()
	if err := s.authStore.Save(ctx, binding); err != nil {
		return "Не удалось сохранить привязку Telegram."
	}
	return fmt.Sprintf("Telegram-чат привязан.\nПользователь: %s\nUser ID: %s", binding.Username, binding.UserID)
}

func (s *service) telegramLogoutText(ctx context.Context, chatID int64) string {
	if err := s.authStore.Delete(ctx, strconv.FormatInt(chatID, 10)); err != nil {
		return "Не удалось выполнить выход."
	}
	return "Telegram-чат отвязан."
}

func (s *service) telegramWhoAmIText(ctx context.Context, chatID int64) string {
	binding, ok, err := s.resolveTelegramBinding(ctx, chatID)
	if err != nil {
		return "Ошибка проверки авторизации."
	}
	if !ok {
		return "Нет авторизации. Сначала выполни /login <username> <password>."
	}
	return fmt.Sprintf("Привязанный пользователь: %s\nUser ID: %s\nРоли: %s", binding.Username, binding.UserID, strings.Join(binding.Roles, ", "))
}

func (s *service) telegramReportText(ctx context.Context, chatID int64, period string) string {
	userID, ok, err := s.resolveTelegramUserID(ctx, chatID)
	if err != nil {
		return "Ошибка проверки авторизации."
	}
	if !ok {
		return "Нет авторизации. Сначала выполни /login <username> <password>."
	}
	from, to := reportWindow(period)
	query := fmt.Sprintf("%s/api/v1/analytics/projections/summary?from=%s&to=%s", s.analyticsServiceURL, from.Format("2006-01-02"), to.Format("2006-01-02"))

	response, err := s.fetchAnalytics(ctx, query, userID)
	if err != nil {
		return "Не удалось построить отчет."
	}
	if len(response.Result.Data) == 0 {
		return "Нет данных за выбранный период."
	}

	row := response.Result.Data[0]
	return fmt.Sprintf(
		"Отчет %s\nРасход: %s\nДоход: %s\nТранзакций: %s\nКатегорий: %s",
		periodLabel(period),
		valueString(row["debit_cents"]),
		valueString(row["credit_cents"]),
		valueString(row["transaction_count"]),
		valueString(row["category_count"]),
	)
}

func (s *service) telegramAlertsText(ctx context.Context, chatID int64) string {
	userID, ok, err := s.resolveTelegramUserID(ctx, chatID)
	if err != nil {
		return "Ошибка проверки авторизации."
	}
	if !ok {
		return "Нет авторизации. Сначала выполни /login <username> <password>."
	}
	now := time.Now().UTC()
	query := fmt.Sprintf("%s/api/v1/analytics/projections/alerts?from=%s&to=%s", s.analyticsServiceURL, now.Format("2006-01-02"), now.Format("2006-01-02"))

	response, err := s.fetchAnalytics(ctx, query, userID)
	if err != nil {
		return "Не удалось получить сводку по алертам."
	}
	if len(response.Result.Data) == 0 {
		return "На сегодня алертов нет."
	}

	lines := []string{"Алерты за сегодня:"}
	for _, row := range response.Result.Data {
		lines = append(lines, fmt.Sprintf("- %s / %s: %s", valueString(row["type"]), valueString(row["severity"]), valueString(row["alert_count"])))
	}
	return strings.Join(lines, "\n")
}

func (s *service) telegramTransactionsText(ctx context.Context, chatID int64, rawLimit string) string {
	userID, ok, err := s.resolveTelegramUserID(ctx, chatID)
	if err != nil {
		return "Ошибка проверки авторизации."
	}
	if !ok {
		return "Нет авторизации. Сначала выполни /login <username> <password>."
	}
	limit := 5
	if parsed, err := strconv.Atoi(strings.TrimSpace(rawLimit)); err == nil && parsed > 0 && parsed <= 20 {
		limit = parsed
	}

	query := fmt.Sprintf("%s/api/v1/transactions?limit=%d", s.ledgerServiceURL, limit)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, query, nil)
	if err != nil {
		return "Не удалось запросить транзакции."
	}
	request.Header.Set("X-User-ID", strings.TrimSpace(userID))

	response, err := s.httpClient.Do(request)
	if err != nil {
		return "Не удалось запросить транзакции."
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusMultipleChoices {
		return "Сервис транзакций временно недоступен."
	}

	var payload ledgerTransactionsResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return "Не удалось обработать ответ сервиса транзакций."
	}
	if len(payload.Transactions) == 0 {
		return "Транзакций пока нет."
	}

	lines := []string{"Последние транзакции:"}
	for _, transaction := range payload.Transactions {
		lines = append(lines, fmt.Sprintf("- %s %d %s [%s]", strings.TrimSpace(transaction.Merchant), transaction.AmountCents, strings.TrimSpace(transaction.Currency), strings.TrimSpace(transaction.Category)))
	}
	return strings.Join(lines, "\n")
}

func (s *service) fetchAnalytics(ctx context.Context, endpoint, userID string) (analyticsHTTPResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return analyticsHTTPResponse{}, err
	}
	request.Header.Set("X-User-ID", strings.TrimSpace(userID))

	response, err := s.httpClient.Do(request)
	if err != nil {
		return analyticsHTTPResponse{}, err
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusMultipleChoices {
		return analyticsHTTPResponse{}, fmt.Errorf("analytics status=%d", response.StatusCode)
	}

	var payload analyticsHTTPResponse
	err = json.NewDecoder(response.Body).Decode(&payload)
	return payload, err
}

func (s *service) verifyTelegramCredentials(ctx context.Context, username, password string) (telegramauth.Binding, error) {
	payload, err := json.Marshal(authVerifyRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return telegramauth.Binding{}, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.authServiceURL+"/internal/auth/verify", bytes.NewReader(payload))
	if err != nil {
		return telegramauth.Binding{}, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := s.httpClient.Do(request)
	if err != nil {
		return telegramauth.Binding{}, err
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusMultipleChoices {
		return telegramauth.Binding{}, fmt.Errorf("auth status=%d", response.StatusCode)
	}

	var verified authVerifyResponse
	if err := json.NewDecoder(response.Body).Decode(&verified); err != nil {
		return telegramauth.Binding{}, err
	}
	return telegramauth.Binding{
		UserID:   strings.TrimSpace(verified.UserID),
		Username: strings.TrimSpace(verified.Username),
		Roles:    verified.Roles,
	}, nil
}

func (s *service) resolveTelegramBinding(ctx context.Context, chatID int64) (telegramauth.Binding, bool, error) {
	binding, ok, err := s.authStore.Get(ctx, strconv.FormatInt(chatID, 10))
	if err != nil {
		return telegramauth.Binding{}, false, err
	}
	if ok {
		return binding, true, nil
	}
	if strings.TrimSpace(s.authServiceURL) != "" || strings.TrimSpace(s.telegramDefaultUserID) == "" {
		return telegramauth.Binding{}, false, nil
	}
	return telegramauth.Binding{
		ChatID:   strconv.FormatInt(chatID, 10),
		UserID:   strings.TrimSpace(s.telegramDefaultUserID),
		Username: "default",
		Roles:    []string{"owner"},
	}, true, nil
}

func (s *service) resolveTelegramUserID(ctx context.Context, chatID int64) (string, bool, error) {
	binding, ok, err := s.resolveTelegramBinding(ctx, chatID)
	if err != nil || !ok {
		return "", ok, err
	}
	return strings.TrimSpace(binding.UserID), strings.TrimSpace(binding.UserID) != "", nil
}

func (s *service) sendTelegramText(ctx context.Context, chatID, text string) error {
	return s.deliverTelegram(ctx, rules.NotificationJob{
		Alert: rules.Alert{
			ID:        "telegram-command-response",
			Type:      "telegram_command",
			Severity:  "info",
			Message:   text,
			CreatedAt: time.Now().UTC(),
		},
		Channel:   "telegram",
		ChatID:    chatID,
		CreatedAt: time.Now().UTC(),
		IsDryRun:  false,
	})
}

func parseAllowedChatIDs(raw string, fallback string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result[trimmed] = struct{}{}
		}
	}
	if len(result) == 0 && strings.TrimSpace(fallback) != "" {
		result[strings.TrimSpace(fallback)] = struct{}{}
	}
	return result
}

func (s *service) isAllowedChat(chatID int64) bool {
	if len(s.allowedTelegramChatIDs) == 0 {
		return true
	}
	_, ok := s.allowedTelegramChatIDs[strconv.FormatInt(chatID, 10)]
	return ok
}

func parseTelegramCommand(text string) (string, []string) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 {
		return "", nil
	}
	command := fields[0]
	if at := strings.Index(command, "@"); at >= 0 {
		command = command[:at]
	}
	return strings.ToLower(command), fields[1:]
}

func isSupportedTelegramImport(filename string) bool {
	switch strings.ToLower(strings.TrimSpace(filepath.Ext(filename))) {
	case ".csv", ".pdf", ".txt":
		return true
	default:
		return false
	}
}

func buildTelegramImportAcceptedText(imported ingestImportResponse) string {
	lines := []string{
		"Выписка принята.",
		"Файл: " + firstNonEmpty(imported.Filename, "unknown"),
		"Import ID: " + firstNonEmpty(imported.ImportID, "n/a"),
		"Статус: " + firstNonEmpty(imported.Status, "accepted"),
	}
	if imported.AlreadyExists {
		lines = append(lines, "Дубликат: да")
	}
	return strings.Join(lines, "\n")
}

func buildTelegramImportSummaryText(parsed parsedImportResponse) string {
	return fmt.Sprintf(
		"Выписка обработана.\nФайл: %s\nФормат: %s\nТранзакций: %d\nРасход: %d\nДоход: %d",
		firstNonEmpty(parsed.Filename, "unknown"),
		firstNonEmpty(parsed.Summary.Format, "unknown"),
		parsed.Summary.TransactionCount,
		parsed.Summary.TotalDebitCents,
		parsed.Summary.TotalCreditCents,
	)
}

func reportWindow(period string) (time.Time, time.Time) {
	now := time.Now().UTC()
	switch strings.ToLower(strings.TrimSpace(period)) {
	case "today":
		day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return day, day
	default:
		from := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return from, now
	}
}

func periodLabel(period string) string {
	switch strings.ToLower(strings.TrimSpace(period)) {
	case "today":
		return "сегодня"
	default:
		return "месяц"
	}
}

func valueString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func firstArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *telegramBotState) snapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]any{
		"last_update_id": s.lastUpdateID,
		"last_command":   s.lastCommand,
		"last_chat_id":   s.lastChatID,
		"last_poll_at":   s.lastPollAt,
		"last_error":     s.lastError,
	}
}

func (s *telegramBotState) nextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastUpdateID <= 0 {
		return 0
	}
	return s.lastUpdateID + 1
}

func (s *telegramBotState) setLastUpdateID(updateID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if updateID > s.lastUpdateID {
		s.lastUpdateID = updateID
	}
}

func (s *telegramBotState) setCommand(chatID int64, updateID int64, command string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if updateID > s.lastUpdateID {
		s.lastUpdateID = updateID
	}
	s.lastCommand = command
	s.lastChatID = strconv.FormatInt(chatID, 10)
	s.lastPollAt = time.Now().UTC()
	s.lastError = ""
}

func (s *telegramBotState) setError(err string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPollAt = time.Now().UTC()
	s.lastError = err
}

func (s *telegramBotState) touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPollAt = time.Now().UTC()
	s.lastError = ""
}

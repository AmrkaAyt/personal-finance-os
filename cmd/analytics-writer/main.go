package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/platform/clickhousex"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/startupx"
	"personal-finance-os/internal/platform/userctx"
	"personal-finance-os/internal/rules"
)

const (
	transactionEventsTable = "transaction_events"
	alertEventsTable       = "alert_events"
	defaultAnalyticsUserID = "user-demo"
)

type service struct {
	logger            *slog.Logger
	clickhouse        *clickhousex.Client
	transactionReader *kafka.Reader
	alertReader       *kafka.Reader
	requestTimeout    time.Duration
	database          string
	transactionTable  string
	alertTable        string
	transactionTopic  string
	alertTopic        string
	transactionGroup  string
	alertGroup        string
}

type transactionEventRow struct {
	EventID        string `json:"event_id"`
	EventDate      string `json:"event_date"`
	UserID         string `json:"user_id"`
	Category       string `json:"category"`
	Merchant       string `json:"merchant"`
	AmountCents    int64  `json:"amount_cents"`
	DebitCents     uint64 `json:"debit_cents"`
	CreditCents    uint64 `json:"credit_cents"`
	Currency       string `json:"currency"`
	TransactionID  string `json:"transaction_id"`
	SourceImportID string `json:"source_import_id"`
	OccurredAt     string `json:"occurred_at"`
	RecordedAt     string `json:"recorded_at"`
}

type alertEventRow struct {
	EventID        string `json:"event_id"`
	EventDate      string `json:"event_date"`
	UserID         string `json:"user_id"`
	Type           string `json:"type"`
	Severity       string `json:"severity"`
	Category       string `json:"category"`
	Merchant       string `json:"merchant"`
	Message        string `json:"message"`
	AmountCents    int64  `json:"amount_cents"`
	TransactionID  string `json:"transaction_id"`
	SourceImportID string `json:"source_import_id"`
	CreatedAt      string `json:"created_at"`
	RecordedAt     string `json:"recorded_at"`
}

func main() {
	const serviceName = "analytics-writer"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	transactionTopic := env.String("KAFKA_ANALYTICS_TOPIC", "transaction.upserted")
	alertTopic := env.String("KAFKA_ALERT_TOPIC", "alert.created")
	transactionGroup := env.String("KAFKA_TRANSACTION_CONSUMER_GROUP", "analytics-writer-transactions")
	alertGroup := env.String("KAFKA_ALERT_CONSUMER_GROUP", "analytics-writer-alerts")
	clickhouseDatabase := env.String("CLICKHOUSE_DATABASE", "finance_os")

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	clickhouseClient, err := clickhousex.New(env.String("CLICKHOUSE_DSN", "http://finance:finance@localhost:8123"), requestTimeout)
	if err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "clickhouse ping", func(ctx context.Context) error {
		return clickhouseClient.Ping(ctx)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "clickhouse ensure analytics schema", func(ctx context.Context) error {
		return ensureSchema(ctx, clickhouseClient, clickhouseDatabase)
	}); err != nil {
		panic(err)
	}

	if err := startupx.Retry(startupCtx, logger, "kafka broker ping", func(ctx context.Context) error {
		return kafkax.Ping(ctx, kafkaBrokers)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "kafka ensure transaction topic", func(ctx context.Context) error {
		return kafkax.EnsureTopic(ctx, kafkaBrokers, transactionTopic, 1, 1)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "kafka ensure alert topic", func(ctx context.Context) error {
		return kafkax.EnsureTopic(ctx, kafkaBrokers, alertTopic, 1, 1)
	}); err != nil {
		panic(err)
	}

	transactionReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers,
		GroupID:  transactionGroup,
		Topic:    transactionTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() {
		_ = transactionReader.Close()
	}()

	alertReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers,
		GroupID:  alertGroup,
		Topic:    alertTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() {
		_ = alertReader.Close()
	}()

	svc := &service{
		logger:            logger,
		clickhouse:        clickhouseClient,
		transactionReader: transactionReader,
		alertReader:       alertReader,
		requestTimeout:    requestTimeout,
		database:          sanitizeIdentifier(clickhouseDatabase),
		transactionTable:  qualifiedTable(clickhouseDatabase, transactionEventsTable),
		alertTable:        qualifiedTable(clickhouseDatabase, alertEventsTable),
		transactionTopic:  transactionTopic,
		alertTopic:        alertTopic,
		transactionGroup:  transactionGroup,
		alertGroup:        alertGroup,
	}

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)
	mux.HandleFunc("GET /api/v1/analytics/projections", svc.handleMetadata)
	mux.HandleFunc("GET /api/v1/analytics/projections/daily-spend", svc.handleDailySpend)
	mux.HandleFunc("GET /api/v1/analytics/projections/alerts", svc.handleAlerts)
	mux.HandleFunc("GET /api/v1/analytics/projections/summary", svc.handleSummary)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8087"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			svc.consumeTransactions,
			svc.consumeAlerts,
		},
	}); err != nil {
		panic(err)
	}
}

func (s *service) handleMetadata(w http.ResponseWriter, _ *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"topics": map[string]string{
			"transactions": s.transactionTopic,
			"alerts":       s.alertTopic,
		},
		"consumer_groups": map[string]string{
			"transactions": s.transactionGroup,
			"alerts":       s.alertGroup,
		},
		"database": s.database,
		"tables":   []string{s.transactionTable, s.alertTable},
		"endpoints": []string{
			"/api/v1/analytics/projections/daily-spend",
			"/api/v1/analytics/projections/alerts",
			"/api/v1/analytics/projections/summary",
		},
	})
}

func (s *service) handleDailySpend(w http.ResponseWriter, r *http.Request) {
	userID, from, to, err := s.parseCommonFilters(r)
	if err != nil {
		status := http.StatusBadRequest
		if err.Error() == "unauthorized" {
			status = http.StatusUnauthorized
		}
		httpx.JSON(w, status, map[string]string{"error": err.Error()})
		return
	}

	conditions := []string{
		fmt.Sprintf("user_id = '%s'", escapeSQLString(userID)),
		fmt.Sprintf("event_date BETWEEN toDate('%s') AND toDate('%s')", from.Format("2006-01-02"), to.Format("2006-01-02")),
	}
	if category := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("category"))); category != "" {
		conditions = append(conditions, fmt.Sprintf("category = '%s'", escapeSQLString(category)))
	}

	query := fmt.Sprintf(`
SELECT
    event_date,
    category,
    sum(debit_cents) AS debit_cents,
    sum(credit_cents) AS credit_cents,
    uniqExact(transaction_id) AS transaction_count
FROM %s FINAL
WHERE %s
GROUP BY event_date, category
ORDER BY event_date ASC, category ASC
`, s.transactionTable, strings.Join(conditions, " AND "))

	s.respondWithQueryResult(w, r, map[string]any{
		"user_id":  userID,
		"from":     from.Format("2006-01-02"),
		"to":       to.Format("2006-01-02"),
		"category": strings.TrimSpace(strings.ToLower(r.URL.Query().Get("category"))),
	}, query)
}

func (s *service) handleAlerts(w http.ResponseWriter, r *http.Request) {
	userID, from, to, err := s.parseCommonFilters(r)
	if err != nil {
		status := http.StatusBadRequest
		if err.Error() == "unauthorized" {
			status = http.StatusUnauthorized
		}
		httpx.JSON(w, status, map[string]string{"error": err.Error()})
		return
	}

	conditions := []string{
		fmt.Sprintf("user_id = '%s'", escapeSQLString(userID)),
		fmt.Sprintf("event_date BETWEEN toDate('%s') AND toDate('%s')", from.Format("2006-01-02"), to.Format("2006-01-02")),
	}
	if severity := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("severity"))); severity != "" {
		conditions = append(conditions, fmt.Sprintf("severity = '%s'", escapeSQLString(severity)))
	}

	query := fmt.Sprintf(`
SELECT
    event_date,
    type,
    severity,
    count() AS alert_count,
    uniqExact(event_id) AS unique_alerts
FROM %s FINAL
WHERE %s
GROUP BY event_date, type, severity
ORDER BY event_date ASC, type ASC, severity ASC
`, s.alertTable, strings.Join(conditions, " AND "))

	s.respondWithQueryResult(w, r, map[string]any{
		"user_id":  userID,
		"from":     from.Format("2006-01-02"),
		"to":       to.Format("2006-01-02"),
		"severity": strings.TrimSpace(strings.ToLower(r.URL.Query().Get("severity"))),
	}, query)
}

func (s *service) handleSummary(w http.ResponseWriter, r *http.Request) {
	userID, from, to, err := s.parseCommonFilters(r)
	if err != nil {
		status := http.StatusBadRequest
		if err.Error() == "unauthorized" {
			status = http.StatusUnauthorized
		}
		httpx.JSON(w, status, map[string]string{"error": err.Error()})
		return
	}

	query := fmt.Sprintf(`
SELECT
    toInt64(sum(debit_cents)) AS debit_cents,
    toInt64(sum(credit_cents)) AS credit_cents,
    uniqExact(transaction_id) AS transaction_count,
    uniqExact(category) AS category_count
FROM %s FINAL
WHERE user_id = '%s'
  AND event_date BETWEEN toDate('%s') AND toDate('%s')
`, s.transactionTable, escapeSQLString(userID), from.Format("2006-01-02"), to.Format("2006-01-02"))

	s.respondWithQueryResult(w, r, map[string]any{
		"user_id": userID,
		"from":    from.Format("2006-01-02"),
		"to":      to.Format("2006-01-02"),
	}, query)
}

func (s *service) respondWithQueryResult(w http.ResponseWriter, r *http.Request, filters map[string]any, query string) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	body, err := s.clickhouse.QueryJSON(ctx, query)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}

	httpx.JSON(w, http.StatusOK, map[string]any{
		"filters": filters,
		"result":  payload,
	})
}

func (s *service) parseCommonFilters(r *http.Request) (string, time.Time, time.Time, error) {
	userID, err := userctx.RequireAuthenticatedUserID(r)
	if err != nil {
		return "", time.Time{}, time.Time{}, fmt.Errorf("unauthorized")
	}

	now := time.Now().UTC()
	from := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	to := now

	if rawFrom := strings.TrimSpace(r.URL.Query().Get("from")); rawFrom != "" {
		parsed, err := time.Parse("2006-01-02", rawFrom)
		if err != nil {
			return "", time.Time{}, time.Time{}, fmt.Errorf("invalid from date")
		}
		from = parsed.UTC()
	}
	if rawTo := strings.TrimSpace(r.URL.Query().Get("to")); rawTo != "" {
		parsed, err := time.Parse("2006-01-02", rawTo)
		if err != nil {
			return "", time.Time{}, time.Time{}, fmt.Errorf("invalid to date")
		}
		to = parsed.UTC()
	}
	if to.Before(from) {
		return "", time.Time{}, time.Time{}, fmt.Errorf("to date must be on or after from date")
	}
	return userID, from, to, nil
}

func (s *service) consumeTransactions(ctx context.Context, logger *slog.Logger) error {
	logger.Info("analytics transaction consumer ready", "topic", s.transactionTopic, "table", s.transactionTable)
	for {
		message, err := s.transactionReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := s.handleTransactionMessage(ctx, message); err != nil {
			return err
		}
		if err := s.transactionReader.CommitMessages(ctx, message); err != nil {
			return err
		}
	}
}

func (s *service) consumeAlerts(ctx context.Context, logger *slog.Logger) error {
	logger.Info("analytics alert consumer ready", "topic", s.alertTopic, "table", s.alertTable)
	for {
		message, err := s.alertReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := s.handleAlertMessage(ctx, message); err != nil {
			return err
		}
		if err := s.alertReader.CommitMessages(ctx, message); err != nil {
			return err
		}
	}
}

func (s *service) handleTransactionMessage(ctx context.Context, message kafka.Message) error {
	var event ledger.TransactionUpsertedEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return err
	}

	operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	row := newTransactionEventRow(event)
	if err := s.clickhouse.InsertJSONEachRow(operationCtx, s.transactionTable, []any{row}); err != nil {
		return err
	}

	s.logger.Info("transaction projection written", "event_id", row.EventID, "user_id", row.UserID, "category", row.Category)
	return nil
}

func (s *service) handleAlertMessage(ctx context.Context, message kafka.Message) error {
	var alert rules.Alert
	if err := json.Unmarshal(message.Value, &alert); err != nil {
		return err
	}

	operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	row := newAlertEventRow(alert)
	if err := s.clickhouse.InsertJSONEachRow(operationCtx, s.alertTable, []any{row}); err != nil {
		return err
	}

	s.logger.Info("alert projection written", "event_id", row.EventID, "user_id", row.UserID, "type", row.Type)
	return nil
}

func ensureSchema(ctx context.Context, client *clickhousex.Client, database string) error {
	qualifiedDatabase := sanitizeIdentifier(database)
	transactionTableName := qualifiedTable(qualifiedDatabase, transactionEventsTable)
	alertTableName := qualifiedTable(qualifiedDatabase, alertEventsTable)

	queries := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", qualifiedDatabase),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    event_id String,
    event_date Date,
    user_id String,
    category LowCardinality(String),
    merchant String,
    amount_cents Int64,
    debit_cents UInt64,
    credit_cents UInt64,
    currency LowCardinality(String),
    transaction_id String,
    source_import_id String,
    occurred_at DateTime64(3, 'UTC'),
    recorded_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(recorded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, event_date, event_id)
`, transactionTableName),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    event_id String,
    event_date Date,
    user_id String,
    type LowCardinality(String),
    severity LowCardinality(String),
    category LowCardinality(String),
    merchant String,
    message String,
    amount_cents Int64,
    transaction_id String,
    source_import_id String,
    created_at DateTime64(3, 'UTC'),
    recorded_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(recorded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, event_date, event_id)
`, alertTableName),
	}

	for _, query := range queries {
		if err := client.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

func newTransactionEventRow(event ledger.TransactionUpsertedEvent) transactionEventRow {
	occurredAt := event.OccurredAt.UTC()
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	debitCents, creditCents := splitTransactionAmounts(event.Category, event.AmountCents)

	return transactionEventRow{
		EventID:        firstNonEmpty(strings.TrimSpace(event.TransactionID), strings.TrimSpace(event.TransactionHash)),
		EventDate:      occurredAt.Format("2006-01-02"),
		UserID:         firstNonEmpty(strings.TrimSpace(event.UserID), defaultAnalyticsUserID),
		Category:       normalizeCategory(event.Category),
		Merchant:       strings.TrimSpace(strings.ToLower(event.Merchant)),
		AmountCents:    event.AmountCents,
		DebitCents:     debitCents,
		CreditCents:    creditCents,
		Currency:       strings.TrimSpace(strings.ToUpper(event.Currency)),
		TransactionID:  strings.TrimSpace(event.TransactionID),
		SourceImportID: strings.TrimSpace(event.SourceImportID),
		OccurredAt:     formatDateTime(occurredAt),
		RecordedAt:     formatDateTime(time.Now().UTC()),
	}
}

func newAlertEventRow(alert rules.Alert) alertEventRow {
	createdAt := alert.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	return alertEventRow{
		EventID:        strings.TrimSpace(alert.ID),
		EventDate:      createdAt.Format("2006-01-02"),
		UserID:         firstNonEmpty(strings.TrimSpace(alert.UserID), defaultAnalyticsUserID),
		Type:           strings.TrimSpace(alert.Type),
		Severity:       strings.TrimSpace(alert.Severity),
		Category:       normalizeCategory(alert.Category),
		Merchant:       strings.TrimSpace(strings.ToLower(alert.Merchant)),
		Message:        strings.TrimSpace(alert.Message),
		AmountCents:    alert.AmountCents,
		TransactionID:  strings.TrimSpace(alert.TransactionID),
		SourceImportID: strings.TrimSpace(alert.SourceImportID),
		CreatedAt:      formatDateTime(createdAt),
		RecordedAt:     formatDateTime(time.Now().UTC()),
	}
}

func splitTransactionAmounts(category string, amountCents int64) (uint64, uint64) {
	absolute := amountCents
	if absolute < 0 {
		absolute = -absolute
	}
	if normalizeCategory(category) == "income" {
		return 0, uint64(absolute)
	}
	return uint64(absolute), 0
}

func formatDateTime(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05.000")
}

func normalizeCategory(value string) string {
	normalized := strings.TrimSpace(strings.ToLower(value))
	if normalized == "" {
		return "uncategorized"
	}
	return normalized
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}

func qualifiedTable(database, table string) string {
	return sanitizeIdentifier(database) + "." + sanitizeIdentifier(table)
}

func sanitizeIdentifier(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "default"
	}
	var builder strings.Builder
	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' {
			builder.WriteRune(char)
		}
	}
	if builder.Len() == 0 {
		return "default"
	}
	return builder.String()
}

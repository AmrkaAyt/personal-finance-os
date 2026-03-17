package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"personal-finance-os/internal/imports"
	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/mongox"
	"personal-finance-os/internal/platform/postgresx"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/startupx"
	"personal-finance-os/internal/platform/userctx"
)

type service struct {
	logger           *slog.Logger
	repository       *ledger.Repository
	parsedCollection *mongo.Collection
	kafkaReader      *kafka.Reader
	kafkaWriter      *kafka.Writer
	quarantineWriter *kafka.Writer
	requestTimeout   time.Duration
	processTimeout   time.Duration
	defaultAccountID string
	consumeTopic     string
	publishTopic     string
	consumerGroup    string
	quarantineTopic  string
	retryBackoff     time.Duration
	maxAttempts      int
	outboxBatchSize  int
	outboxPollDelay  time.Duration
	outboxClaimTTL   time.Duration
	outboxOwner      string
}

type createTransactionRequest struct {
	AccountID   string     `json:"account_id"`
	Merchant    string     `json:"merchant"`
	Category    string     `json:"category"`
	AmountCents int64      `json:"amount_cents"`
	Currency    string     `json:"currency"`
	OccurredAt  *time.Time `json:"occurred_at,omitempty"`
}

func main() {
	const serviceName = "ledger-service"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	processTimeout := env.Duration("PROCESSING_TIMEOUT", 2*time.Minute)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)
	postgresDSN := env.String("POSTGRES_DSN", "postgres://finance:finance@localhost:5432/finance?sslmode=disable")
	mongoURI := env.String("MONGO_URI", "mongodb://localhost:27017")
	mongoDatabase := env.String("MONGO_DATABASE", "finance_os")
	parsedCollectionName := env.String("MONGO_PARSED_COLLECTION", "parsed_imports")
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	consumeTopic := env.String("KAFKA_PARSED_TOPIC", "statement.parsed")
	publishTopic := env.String("KAFKA_TRANSACTION_TOPIC", "transaction.upserted")
	consumerGroup := env.String("KAFKA_CONSUMER_GROUP", "ledger-service")
	quarantineTopic := env.String("KAFKA_QUARANTINE_TOPIC", "event.quarantine")
	retryBackoff := env.Duration("KAFKA_CONSUMER_RETRY_BACKOFF", 2*time.Second)
	maxAttempts := env.Int("KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS", 3)
	defaultAccountID := env.String("LEDGER_DEFAULT_ACCOUNT_ID", ledger.DefaultAccountID)
	outboxBatchSize := env.Int("LEDGER_OUTBOX_BATCH_SIZE", 100)
	outboxPollDelay := env.Duration("LEDGER_OUTBOX_POLL_DELAY", time.Second)
	outboxClaimTTL := env.Duration("LEDGER_OUTBOX_CLAIM_TTL", 30*time.Second)
	hostname, _ := os.Hostname()
	outboxOwner := fmt.Sprintf("%s-%s-%d", serviceName, strings.TrimSpace(hostname), time.Now().UTC().UnixNano())

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	postgresPool, err := startupx.RetryValue(startupCtx, logger, "postgres connect", func(ctx context.Context) (*pgxpool.Pool, error) {
		return postgresx.Connect(ctx, postgresDSN)
	})
	if err != nil {
		panic(err)
	}
	defer postgresPool.Close()

	repository := ledger.NewRepository(postgresPool)

	mongoClient, err := startupx.RetryValue(startupCtx, logger, "mongodb connect", func(ctx context.Context) (*mongo.Client, error) {
		return mongox.Connect(ctx, mongoURI)
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = mongoClient.Disconnect(context.Background())
	}()
	parsedCollection := mongoClient.Database(mongoDatabase).Collection(parsedCollectionName)
	if err := startupx.Retry(startupCtx, logger, "mongodb update parsed import indexes", func(ctx context.Context) error {
		_ = mongox.DropIndex(ctx, parsedCollection, "import_id_1")
		return mongox.EnsureUniqueCompoundIndex(ctx, parsedCollection, "user_id", "import_id")
	}); err != nil {
		panic(err)
	}

	if err := startupx.Retry(startupCtx, logger, "kafka broker ping", func(ctx context.Context) error {
		return kafkax.Ping(ctx, kafkaBrokers)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "kafka ensure transaction topic", func(ctx context.Context) error {
		return kafkax.EnsureTopic(ctx, kafkaBrokers, publishTopic, 1, 1)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "kafka ensure quarantine topic", func(ctx context.Context) error {
		return kafkax.EnsureTopic(ctx, kafkaBrokers, quarantineTopic, 1, 1)
	}); err != nil {
		panic(err)
	}

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaBrokers,
		GroupID:  consumerGroup,
		Topic:    consumeTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() {
		_ = kafkaReader.Close()
	}()
	kafkaWriter := kafkax.NewWriter(kafkaBrokers, publishTopic)
	defer func() {
		_ = kafkaWriter.Close()
	}()
	quarantineWriter := kafkax.NewWriter(kafkaBrokers, quarantineTopic)
	defer func() {
		_ = quarantineWriter.Close()
	}()

	svc := &service{
		logger:           logger,
		repository:       repository,
		parsedCollection: parsedCollection,
		kafkaReader:      kafkaReader,
		kafkaWriter:      kafkaWriter,
		quarantineWriter: quarantineWriter,
		requestTimeout:   requestTimeout,
		processTimeout:   processTimeout,
		defaultAccountID: defaultAccountID,
		consumeTopic:     consumeTopic,
		publishTopic:     publishTopic,
		consumerGroup:    consumerGroup,
		quarantineTopic:  quarantineTopic,
		retryBackoff:     retryBackoff,
		maxAttempts:      maxAttempts,
		outboxBatchSize:  outboxBatchSize,
		outboxPollDelay:  outboxPollDelay,
		outboxClaimTTL:   outboxClaimTTL,
		outboxOwner:      outboxOwner,
	}

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)
	mux.HandleFunc("GET /api/v1/transactions", svc.handleListTransactions)
	mux.HandleFunc("POST /api/v1/transactions", svc.handleCreateTransaction)
	mux.HandleFunc("GET /api/v1/categories", svc.handleListCategories)
	mux.HandleFunc("GET /api/v1/recurring", svc.handleListRecurring)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8084"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			svc.consumeParsedEvents,
			svc.publishOutboxEvents,
		},
	}); err != nil {
		panic(err)
	}
}

func (s *service) handleListTransactions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	userID, err := userctx.RequireAuthenticatedUserID(r)
	if err != nil {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	limit := 200
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid limit"})
			return
		}
		if parsed > 200 {
			parsed = 200
		}
		limit = parsed
	}

	transactions, err := s.repository.ListTransactions(ctx, userID, limit)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"transactions": transactions})
}

func (s *service) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	userID, err := userctx.RequireAuthenticatedUserID(r)
	if err != nil {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	}
	if idempotencyKey == "" {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "missing_idempotency_key"})
		return
	}

	var input createTransactionRequest
	if err := httpx.ReadJSON(r, &input); err != nil {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}
	transaction := ledgerManualTransactionFromRequest(userID, s.defaultAccountID, idempotencyKey, input)

	stored, err := s.repository.UpsertTransactionWithOutbox(ctx, transaction, s.publishTopic)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	httpx.JSON(w, http.StatusCreated, stored)
}

func (s *service) handleListCategories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	if _, err := userctx.RequireAuthenticatedUserID(r); err != nil {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	categories, err := s.repository.ListCategories(ctx)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"categories": categories})
}

func (s *service) handleListRecurring(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	userID, err := userctx.RequireAuthenticatedUserID(r)
	if err != nil {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	transactions, err := s.repository.ListTransactions(ctx, userID, 1000)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	patterns := ledger.DetectRecurring(transactions)
	httpx.JSON(w, http.StatusOK, map[string]any{"patterns": patterns})
}

func (s *service) consumeParsedEvents(ctx context.Context, logger *slog.Logger) error {
	logger.Info("ledger parsed consumer ready", "topic", s.consumeTopic, "publish_topic", s.publishTopic)
	return kafkax.ConsumeLoop(ctx, kafkax.ConsumerOptions{
		Name:             "ledger-parsed-consumer",
		Reader:           s.kafkaReader,
		QuarantineWriter: s.quarantineWriter,
		Handler:          s.handleParsedMessage,
		Logger:           logger,
		ConsumerGroup:    s.consumerGroup,
		RetryBackoff:     s.retryBackoff,
		MaxAttempts:      s.maxAttempts,
	})
}

func (s *service) handleParsedMessage(ctx context.Context, message kafka.Message) error {
	var event imports.StatementParsedEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return kafkax.Permanent(err)
	}

	operationCtx, cancel := context.WithTimeout(ctx, s.processTimeout)
	defer cancel()

	parsedImport, err := s.findParsedImport(operationCtx, event.UserID, event.ImportID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return kafkax.Permanent(err)
		}
		return err
	}

	transactions := make([]ledger.Transaction, 0, len(parsedImport.Transactions))
	for _, parsedTransaction := range parsedImport.Transactions {
		resolvedUserID := strings.TrimSpace(parsedImport.UserID)
		if resolvedUserID == "" {
			return kafkax.Permanent(errors.New("parsed import has empty user_id"))
		}
		transactions = append(transactions, ledger.NewTransactionFromParsed(
			resolvedUserID,
			s.defaultAccountID,
			event.ImportID,
			parsedTransaction,
		))
	}
	if len(transactions) == 0 {
		s.logger.Info("parsed import contains no transactions", "import_id", event.ImportID)
		return nil
	}

	stored, err := s.repository.UpsertTransactionsWithOutbox(operationCtx, transactions, s.publishTopic)
	if err != nil {
		return err
	}

	s.logger.Info("ledger import applied", "import_id", event.ImportID, "transactions", len(stored))
	return nil
}

func (s *service) findParsedImport(ctx context.Context, userID, importID string) (imports.ParsedImport, error) {
	var parsedImport imports.ParsedImport
	err := s.parsedCollection.FindOne(ctx, userImportFilter(userID, importID)).Decode(&parsedImport)
	return parsedImport, err
}

func (s *service) emitTransactionEvent(ctx context.Context, transaction ledger.Transaction) error {
	return kafkax.PublishJSON(ctx, s.kafkaWriter, transaction.ID, ledger.NewTransactionUpsertedEvent(transaction))
}

func (s *service) publishOutboxEvents(ctx context.Context, logger *slog.Logger) error {
	logger.Info("ledger outbox publisher ready", "topic", s.publishTopic, "owner", s.outboxOwner)

	ticker := time.NewTicker(s.outboxPollDelay)
	defer ticker.Stop()

	for {
		if err := s.publishOutboxBatch(ctx); err != nil {
			logger.Error("ledger outbox batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *service) publishOutboxBatch(ctx context.Context) error {
	operationCtx, cancel := context.WithTimeout(ctx, s.processTimeout)
	defer cancel()

	events, err := s.repository.ClaimPendingOutboxEvents(operationCtx, s.outboxBatchSize, s.outboxOwner, s.outboxClaimTTL)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	for _, event := range events {
		if err := s.publishClaimedOutboxEvent(ctx, event); err != nil {
			s.logger.Error("ledger outbox publish failed", "event_id", event.ID, "event_type", event.EventType, "error", err)
		}
	}
	return nil
}

func (s *service) publishClaimedOutboxEvent(ctx context.Context, event ledger.OutboxEvent) error {
	publishCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	if err := s.kafkaWriter.WriteMessages(publishCtx, kafka.Message{
		Key:   []byte(event.MessageKey),
		Value: []byte(event.Payload),
		Time:  time.Now().UTC(),
	}); err != nil {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), s.requestTimeout)
		defer releaseCancel()
		releaseErr := s.repository.ReleaseOutboxEventClaim(releaseCtx, event.ID, err.Error())
		if releaseErr != nil {
			s.logger.Error("failed to release outbox claim", "event_id", event.ID, "error", releaseErr)
		}
		return err
	}

	markCtx, markCancel := context.WithTimeout(context.Background(), s.requestTimeout)
	defer markCancel()
	if err := s.repository.MarkOutboxEventPublished(markCtx, event.ID); err != nil {
		return err
	}

	s.logger.Info("ledger outbox event published", "event_id", event.ID, "event_type", event.EventType, "attempts", event.PublishAttempts)
	return nil
}

func ledgerManualTransactionFromRequest(userID, defaultAccountID, idempotencyKey string, input createTransactionRequest) ledger.Transaction {
	occurredAt := time.Now().UTC()
	if input.OccurredAt != nil && !input.OccurredAt.IsZero() {
		occurredAt = input.OccurredAt.UTC()
	}

	transaction := ledger.Transaction{
		UserID:         strings.TrimSpace(userID),
		AccountID:      firstNonEmpty(strings.TrimSpace(input.AccountID), defaultAccountID),
		SourceImportID: "manual:" + strings.TrimSpace(idempotencyKey),
		Merchant:       strings.TrimSpace(input.Merchant),
		Category:       firstNonEmpty(strings.TrimSpace(input.Category), "uncategorized"),
		AmountCents:    input.AmountCents,
		Currency:       firstNonEmpty(strings.TrimSpace(strings.ToUpper(input.Currency)), "USD"),
		OccurredAt:     occurredAt,
	}
	transaction.Fingerprint = ledger.ManualTransactionFingerprint(transaction.UserID, idempotencyKey)
	transaction.ID = ledger.TransactionID(transaction.UserID, transaction.Fingerprint)
	return transaction
}

func userImportFilter(userID, importID string) bson.M {
	filter := bson.M{"import_id": importID}
	if strings.TrimSpace(userID) != "" {
		filter["user_id"] = strings.TrimSpace(userID)
	}
	return filter
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

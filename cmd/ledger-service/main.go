package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

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
)

type service struct {
	logger           *slog.Logger
	repository       *ledger.Repository
	parsedCollection *mongo.Collection
	kafkaReader      *kafka.Reader
	kafkaWriter      *kafka.Writer
	requestTimeout   time.Duration
	defaultUserID    string
	defaultAccountID string
	consumeTopic     string
	publishTopic     string
}

func main() {
	const serviceName = "ledger-service"

	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	postgresDSN := env.String("POSTGRES_DSN", "postgres://finance:finance@localhost:5432/finance?sslmode=disable")
	mongoURI := env.String("MONGO_URI", "mongodb://localhost:27017")
	mongoDatabase := env.String("MONGO_DATABASE", "finance_os")
	parsedCollectionName := env.String("MONGO_PARSED_COLLECTION", "parsed_imports")
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	consumeTopic := env.String("KAFKA_PARSED_TOPIC", "statement.parsed")
	publishTopic := env.String("KAFKA_TRANSACTION_TOPIC", "transaction.upserted")
	consumerGroup := env.String("KAFKA_CONSUMER_GROUP", "ledger-service")
	defaultUserID := env.String("LEDGER_DEFAULT_USER_ID", ledger.DefaultUserID)
	defaultAccountID := env.String("LEDGER_DEFAULT_ACCOUNT_ID", ledger.DefaultAccountID)

	startupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	postgresPool, err := postgresx.Connect(startupCtx, postgresDSN)
	if err != nil {
		panic(err)
	}
	defer postgresPool.Close()

	repository := ledger.NewRepository(postgresPool)
	if err := repository.EnsureSchema(startupCtx); err != nil {
		panic(err)
	}

	mongoClient, err := mongox.Connect(startupCtx, mongoURI)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = mongoClient.Disconnect(context.Background())
	}()
	parsedCollection := mongoClient.Database(mongoDatabase).Collection(parsedCollectionName)
	if err := mongox.EnsureUniqueIndex(startupCtx, parsedCollection, "import_id"); err != nil {
		panic(err)
	}

	if err := kafkax.Ping(startupCtx, kafkaBrokers); err != nil {
		panic(err)
	}
	if err := kafkax.EnsureTopic(startupCtx, kafkaBrokers, publishTopic, 1, 1); err != nil {
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

	svc := &service{
		logger:           logger,
		repository:       repository,
		parsedCollection: parsedCollection,
		kafkaReader:      kafkaReader,
		kafkaWriter:      kafkaWriter,
		requestTimeout:   requestTimeout,
		defaultUserID:    defaultUserID,
		defaultAccountID: defaultAccountID,
		consumeTopic:     consumeTopic,
		publishTopic:     publishTopic,
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
		},
	}); err != nil {
		panic(err)
	}
}

func (s *service) handleListTransactions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	limit := 200
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid limit"})
			return
		}
		limit = parsed
	}

	transactions, err := s.repository.ListTransactions(ctx, limit)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"transactions": transactions})
}

func (s *service) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	var transaction ledger.Transaction
	if err := httpx.ReadJSON(r, &transaction); err != nil {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}
	transaction = ledgerNormalizeManualTransaction(transaction, s.defaultUserID, s.defaultAccountID)

	stored, err := s.repository.UpsertTransaction(ctx, transaction)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := s.emitTransactionEvent(ctx, stored); err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpx.JSON(w, http.StatusCreated, stored)
}

func (s *service) handleListCategories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	categories, err := s.repository.ListCategories(ctx)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"categories": categories})
}

func (s *service) handleListRecurring(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	transactions, err := s.repository.ListTransactions(ctx, 1000)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	patterns := ledger.DetectRecurring(transactions)
	httpx.JSON(w, http.StatusOK, map[string]any{"patterns": patterns})
}

func (s *service) consumeParsedEvents(ctx context.Context, logger *slog.Logger) error {
	logger.Info("ledger parsed consumer ready", "topic", s.consumeTopic, "publish_topic", s.publishTopic)

	for {
		message, err := s.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := s.handleParsedMessage(ctx, message); err != nil {
			return err
		}
		if err := s.kafkaReader.CommitMessages(ctx, message); err != nil {
			return err
		}
	}
}

func (s *service) handleParsedMessage(ctx context.Context, message kafka.Message) error {
	var event imports.StatementParsedEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return err
	}

	operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	parsedImport, err := s.findParsedImport(operationCtx, event.ImportID)
	if err != nil {
		return err
	}

	transactions := make([]ledger.Transaction, 0, len(parsedImport.Transactions))
	for _, parsedTransaction := range parsedImport.Transactions {
		transactions = append(transactions, ledger.NewTransactionFromParsed(
			s.defaultUserID,
			s.defaultAccountID,
			event.ImportID,
			parsedTransaction,
		))
	}
	if len(transactions) == 0 {
		s.logger.Info("parsed import contains no transactions", "import_id", event.ImportID)
		return nil
	}

	stored, err := s.repository.UpsertTransactions(operationCtx, transactions)
	if err != nil {
		return err
	}
	for _, transaction := range stored {
		if err := s.emitTransactionEvent(operationCtx, transaction); err != nil {
			return err
		}
	}

	s.logger.Info("ledger import applied", "import_id", event.ImportID, "transactions", len(stored))
	return nil
}

func (s *service) findParsedImport(ctx context.Context, importID string) (imports.ParsedImport, error) {
	var parsedImport imports.ParsedImport
	err := s.parsedCollection.FindOne(ctx, bson.M{"import_id": importID}).Decode(&parsedImport)
	return parsedImport, err
}

func (s *service) emitTransactionEvent(ctx context.Context, transaction ledger.Transaction) error {
	return kafkax.PublishJSON(ctx, s.kafkaWriter, transaction.ID, ledger.NewTransactionUpsertedEvent(transaction))
}

func ledgerNormalizeManualTransaction(transaction ledger.Transaction, defaultUserID, defaultAccountID string) ledger.Transaction {
	if transaction.UserID == "" {
		transaction.UserID = defaultUserID
	}
	if transaction.AccountID == "" {
		transaction.AccountID = defaultAccountID
	}
	if transaction.SourceImportID == "" {
		transaction.SourceImportID = "manual"
	}
	if transaction.Category == "" {
		transaction.Category = "uncategorized"
	}
	if transaction.Currency == "" {
		transaction.Currency = "USD"
	}
	if transaction.OccurredAt.IsZero() {
		transaction.OccurredAt = time.Now().UTC()
	}
	if transaction.Fingerprint == "" {
		transaction.Fingerprint = ledger.TransactionFingerprintFromLedger(transaction)
	}
	if transaction.ID == "" {
		transaction.ID = "txn-" + transaction.Fingerprint[:24]
	}
	return transaction
}

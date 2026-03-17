package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"personal-finance-os/internal/imports"
	parserdomain "personal-finance-os/internal/parser"
	"personal-finance-os/internal/platform/cryptox"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/mongox"
	"personal-finance-os/internal/platform/rabbitmq"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/startupx"
	"personal-finance-os/internal/platform/userctx"
)

type service struct {
	logger           *slog.Logger
	rawCollection    *mongo.Collection
	parsedCollection *mongo.Collection
	rabbitConn       *amqp.Connection
	parseQueue       string
	parsedTopic      string
	kafkaWriter      *kafka.Writer
	requestTimeout   time.Duration
	fieldCipher      *cryptox.FieldCipher
}

func main() {
	const serviceName = "parser-service"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)
	mongoURI := env.String("MONGO_URI", "mongodb://localhost:27017")
	mongoDatabase := env.String("MONGO_DATABASE", "finance_os")
	rawCollectionName := env.String("MONGO_RAW_COLLECTION", "raw_imports")
	parsedCollectionName := env.String("MONGO_PARSED_COLLECTION", "parsed_imports")
	rabbitURL := env.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	parseQueue := env.String("RABBIT_PARSE_QUEUE", "parse.statement")
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	parsedTopic := env.String("KAFKA_PARSED_TOPIC", "statement.parsed")
	fieldCipher, err := cryptox.NewFieldCipherFromBase64(env.String("DATA_ENCRYPTION_KEY_B64", ""))
	if err != nil {
		panic(err)
	}

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	mongoClient, err := startupx.RetryValue(startupCtx, logger, "mongodb connect", func(ctx context.Context) (*mongo.Client, error) {
		return mongox.Connect(ctx, mongoURI)
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = mongoClient.Disconnect(context.Background())
	}()

	rawCollection := mongoClient.Database(mongoDatabase).Collection(rawCollectionName)
	parsedCollection := mongoClient.Database(mongoDatabase).Collection(parsedCollectionName)
	if err := startupx.Retry(startupCtx, logger, "mongodb update raw import indexes", func(ctx context.Context) error {
		_ = mongox.DropIndex(ctx, rawCollection, "import_id_1")
		return mongox.EnsureUniqueCompoundIndex(ctx, rawCollection, "user_id", "import_id")
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "mongodb update parsed import indexes", func(ctx context.Context) error {
		_ = mongox.DropIndex(ctx, parsedCollection, "import_id_1")
		return mongox.EnsureUniqueCompoundIndex(ctx, parsedCollection, "user_id", "import_id")
	}); err != nil {
		panic(err)
	}

	rabbitConn, err := startupx.RetryValue(startupCtx, logger, "rabbitmq connect", func(context.Context) (*amqp.Connection, error) {
		return rabbitmq.Connect(rabbitURL)
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rabbitConn.Close()
	}()
	declareChannel, err := startupx.RetryValue(startupCtx, logger, "rabbitmq declare parse queue", func(context.Context) (*amqp.Channel, error) {
		channel, err := rabbitmq.OpenChannel(rabbitConn)
		if err != nil {
			return nil, err
		}
		if err := rabbitmq.DeclareWorkQueue(channel, parseQueue); err != nil {
			_ = channel.Close()
			return nil, err
		}
		return channel, nil
	})
	if err != nil {
		panic(err)
	}
	_ = declareChannel.Close()

	if err := startupx.Retry(startupCtx, logger, "kafka broker ping", func(ctx context.Context) error {
		return kafkax.Ping(ctx, kafkaBrokers)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "kafka ensure parsed topic", func(ctx context.Context) error {
		return kafkax.EnsureTopic(ctx, kafkaBrokers, parsedTopic, 1, 1)
	}); err != nil {
		panic(err)
	}
	kafkaWriter := kafkax.NewWriter(kafkaBrokers, parsedTopic)
	defer func() {
		_ = kafkaWriter.Close()
	}()

	svc := &service{
		logger:           logger,
		rawCollection:    rawCollection,
		parsedCollection: parsedCollection,
		rabbitConn:       rabbitConn,
		parseQueue:       parseQueue,
		parsedTopic:      parsedTopic,
		kafkaWriter:      kafkaWriter,
		requestTimeout:   requestTimeout,
		fieldCipher:      fieldCipher,
	}

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)
	mux.HandleFunc("GET /parser/normalize/demo", func(w http.ResponseWriter, r *http.Request) {
		merchant := r.URL.Query().Get("merchant")
		httpx.JSON(w, http.StatusOK, map[string]string{
			"input":  merchant,
			"output": parserdomain.NormalizeMerchant(merchant),
		})
	})
	mux.HandleFunc("GET /parser/results/{importID}", svc.handleGetParsedImport)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8083"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			svc.consumeParseQueue,
		},
	}); err != nil {
		panic(err)
	}
}

func (s *service) handleGetParsedImport(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	userID, err := userctx.RequireAuthenticatedUserID(r)
	if err != nil {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	var parsed imports.ParsedImport
	err = s.parsedCollection.FindOne(ctx, userImportFilter(userID, r.PathValue("importID"))).Decode(&parsed)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, mongo.ErrNoDocuments) {
			status = http.StatusNotFound
		}
		publicError := "internal_error"
		if status == http.StatusNotFound {
			publicError = "not_found"
		}
		httpx.JSON(w, status, map[string]string{"error": publicError})
		return
	}

	httpx.JSON(w, http.StatusOK, parsed)
}

func (s *service) consumeParseQueue(ctx context.Context, logger *slog.Logger) error {
	channel, err := rabbitmq.OpenChannel(s.rabbitConn)
	if err != nil {
		return err
	}
	defer func() {
		_ = channel.Close()
	}()
	if err := rabbitmq.DeclareWorkQueue(channel, s.parseQueue); err != nil {
		return err
	}
	if err := channel.Qos(1, 0, false); err != nil {
		return err
	}

	messages, err := channel.Consume(s.parseQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	logger.Info("parser worker ready", "queue", s.parseQueue, "topic", s.parsedTopic)

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-messages:
			if !ok {
				return errors.New("rabbitmq delivery channel closed")
			}
			if err := s.handleDelivery(ctx, delivery); err != nil {
				logger.Error("failed to process parse job", "error", err)
				_ = delivery.Nack(false, false)
				continue
			}
			_ = delivery.Ack(false)
		}
	}
}

func (s *service) handleDelivery(ctx context.Context, delivery amqp.Delivery) error {
	var job imports.ParseJob
	if err := json.Unmarshal(delivery.Body, &job); err != nil {
		return err
	}
	return s.processJob(ctx, job)
}

func (s *service) processJob(ctx context.Context, job imports.ParseJob) error {
	operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	rawImport, err := s.findRawImport(operationCtx, job.UserID, job.ImportID)
	if err != nil {
		return err
	}
	content, err := s.decryptRawImport(rawImport)
	if err != nil {
		return err
	}

	parsedImport, err := s.findParsedImport(operationCtx, job.UserID, job.ImportID)
	if err == nil {
		if rawImport.Status == "parsed_pending_event" {
			if err := s.emitParsedEvent(operationCtx, parsedImport); err != nil {
				return err
			}
			s.markRawStatus(context.Background(), job.UserID, job.ImportID, "parsed")
		}
		s.logger.Info("parse job already processed", "import_id", job.ImportID)
		return nil
	}
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}

	s.markRawStatus(context.Background(), job.UserID, job.ImportID, "parsing")
	result := parserdomain.ParseStatement(rawImport.Filename, content)
	parsed := imports.ParsedImport{
		UserID:       rawImport.UserID,
		ImportID:     rawImport.ImportID,
		Filename:     rawImport.Filename,
		Status:       "parsed",
		Summary:      result.Summary,
		Transactions: sanitizeParsedTransactions(result.Transactions),
		ParsedAt:     time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}
	if _, err := s.parsedCollection.InsertOne(operationCtx, parsed); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			s.logger.Info("parse job became idempotent duplicate", "import_id", job.ImportID)
			return nil
		}
		return err
	}
	if err := s.emitParsedEvent(operationCtx, parsed); err != nil {
		s.markRawStatus(context.Background(), job.UserID, job.ImportID, "parsed_pending_event")
		s.logger.Error("failed to emit parsed event", "import_id", job.ImportID, "error", err)
		return nil
	}

	s.markRawStatus(context.Background(), job.UserID, job.ImportID, "parsed")
	s.logger.Info("parse job completed", "import_id", job.ImportID, "transactions", result.Summary.TransactionCount, "format", result.Summary.Format)
	return nil
}

func (s *service) emitParsedEvent(ctx context.Context, parsed imports.ParsedImport) error {
	event := imports.StatementParsedEvent{
		UserID:           parsed.UserID,
		ImportID:         parsed.ImportID,
		Filename:         parsed.Filename,
		Status:           parsed.Status,
		Format:           parsed.Summary.Format,
		TransactionCount: parsed.Summary.TransactionCount,
		ParsedAt:         parsed.ParsedAt,
	}
	return kafkax.PublishJSON(ctx, s.kafkaWriter, parsed.ImportID, event)
}

func (s *service) findRawImport(ctx context.Context, userID, importID string) (imports.RawImport, error) {
	var raw imports.RawImport
	err := s.rawCollection.FindOne(ctx, userImportFilter(userID, importID)).Decode(&raw)
	return raw, err
}

func (s *service) findParsedImport(ctx context.Context, userID, importID string) (imports.ParsedImport, error) {
	var parsed imports.ParsedImport
	err := s.parsedCollection.FindOne(ctx, userImportFilter(userID, importID)).Decode(&parsed)
	return parsed, err
}

func (s *service) markRawStatus(ctx context.Context, userID, importID, status string) {
	updateCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	_, _ = s.rawCollection.UpdateOne(updateCtx, userImportFilter(userID, importID), bson.M{"$set": bson.M{"status": status, "updated_at": time.Now().UTC()}})
}

func (s *service) decryptRawImport(raw imports.RawImport) ([]byte, error) {
	if len(raw.ContentEnc) > 0 {
		return s.fieldCipher.Decrypt(raw.ContentEnc, raw.ContentNnc)
	}
	return raw.Content, nil
}

func sanitizeParsedTransactions(transactions []parserdomain.Transaction) []parserdomain.Transaction {
	sanitized := make([]parserdomain.Transaction, 0, len(transactions))
	for _, transaction := range transactions {
		transaction.RawLine = ""
		sanitized = append(sanitized, transaction)
	}
	return sanitized
}

func userImportFilter(userID, importID string) bson.M {
	filter := bson.M{"import_id": importID}
	if strings.TrimSpace(userID) != "" {
		filter["user_id"] = strings.TrimSpace(userID)
	}
	return filter
}

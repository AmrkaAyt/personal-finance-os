package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"personal-finance-os/internal/imports"
	parserdomain "personal-finance-os/internal/parser"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/mongox"
	"personal-finance-os/internal/platform/rabbitmq"
	"personal-finance-os/internal/platform/runtime"
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
}

func main() {
	const serviceName = "parser-service"

	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	mongoURI := env.String("MONGO_URI", "mongodb://localhost:27017")
	mongoDatabase := env.String("MONGO_DATABASE", "finance_os")
	rawCollectionName := env.String("MONGO_RAW_COLLECTION", "raw_imports")
	parsedCollectionName := env.String("MONGO_PARSED_COLLECTION", "parsed_imports")
	rabbitURL := env.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	parseQueue := env.String("RABBIT_PARSE_QUEUE", "parse.statement")
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	parsedTopic := env.String("KAFKA_PARSED_TOPIC", "statement.parsed")

	startupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	mongoClient, err := mongox.Connect(startupCtx, mongoURI)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = mongoClient.Disconnect(context.Background())
	}()

	rawCollection := mongoClient.Database(mongoDatabase).Collection(rawCollectionName)
	parsedCollection := mongoClient.Database(mongoDatabase).Collection(parsedCollectionName)
	if err := mongox.EnsureUniqueIndex(startupCtx, rawCollection, "import_id"); err != nil {
		panic(err)
	}
	if err := mongox.EnsureUniqueIndex(startupCtx, parsedCollection, "import_id"); err != nil {
		panic(err)
	}

	rabbitConn, err := rabbitmq.Connect(rabbitURL)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rabbitConn.Close()
	}()
	declareChannel, err := rabbitmq.OpenChannel(rabbitConn)
	if err != nil {
		panic(err)
	}
	if err := rabbitmq.DeclareWorkQueue(declareChannel, parseQueue); err != nil {
		panic(err)
	}
	_ = declareChannel.Close()

	if err := kafkax.Ping(startupCtx, kafkaBrokers); err != nil {
		panic(err)
	}
	if err := kafkax.EnsureTopic(startupCtx, kafkaBrokers, parsedTopic, 1, 1); err != nil {
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

	var parsed imports.ParsedImport
	err := s.parsedCollection.FindOne(ctx, bson.M{"import_id": r.PathValue("importID")}).Decode(&parsed)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, mongo.ErrNoDocuments) {
			status = http.StatusNotFound
		}
		httpx.JSON(w, status, map[string]string{"error": err.Error()})
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

	rawImport, err := s.findRawImport(operationCtx, job.ImportID)
	if err != nil {
		return err
	}

	parsedImport, err := s.findParsedImport(operationCtx, job.ImportID)
	if err == nil {
		if rawImport.Status == "parsed_pending_event" {
			if err := s.emitParsedEvent(operationCtx, parsedImport); err != nil {
				return err
			}
			s.markRawStatus(context.Background(), job.ImportID, "parsed")
		}
		s.logger.Info("parse job already processed", "import_id", job.ImportID)
		return nil
	}
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}

	s.markRawStatus(context.Background(), job.ImportID, "parsing")
	result := parserdomain.ParseStatement(rawImport.Filename, rawImport.Content)
	parsed := imports.ParsedImport{
		ImportID:     rawImport.ImportID,
		Filename:     rawImport.Filename,
		Status:       "parsed",
		Summary:      result.Summary,
		Transactions: result.Transactions,
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
		s.markRawStatus(context.Background(), job.ImportID, "parsed_pending_event")
		s.logger.Error("failed to emit parsed event", "import_id", job.ImportID, "error", err)
		return nil
	}

	s.markRawStatus(context.Background(), job.ImportID, "parsed")
	s.logger.Info("parse job completed", "import_id", job.ImportID, "transactions", result.Summary.TransactionCount, "format", result.Summary.Format)
	return nil
}

func (s *service) emitParsedEvent(ctx context.Context, parsed imports.ParsedImport) error {
	event := imports.StatementParsedEvent{
		ImportID:         parsed.ImportID,
		Filename:         parsed.Filename,
		Status:           parsed.Status,
		Format:           parsed.Summary.Format,
		TransactionCount: parsed.Summary.TransactionCount,
		ParsedAt:         parsed.ParsedAt,
	}
	return kafkax.PublishJSON(ctx, s.kafkaWriter, parsed.ImportID, event)
}

func (s *service) findRawImport(ctx context.Context, importID string) (imports.RawImport, error) {
	var raw imports.RawImport
	err := s.rawCollection.FindOne(ctx, bson.M{"import_id": importID}).Decode(&raw)
	return raw, err
}

func (s *service) findParsedImport(ctx context.Context, importID string) (imports.ParsedImport, error) {
	var parsed imports.ParsedImport
	err := s.parsedCollection.FindOne(ctx, bson.M{"import_id": importID}).Decode(&parsed)
	return parsed, err
}

func (s *service) markRawStatus(ctx context.Context, importID, status string) {
	updateCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	_, _ = s.rawCollection.UpdateOne(updateCtx, bson.M{"import_id": importID}, bson.M{"$set": bson.M{"status": status, "updated_at": time.Now().UTC()}})
}

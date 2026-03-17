package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"

	"personal-finance-os/internal/imports"
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
	logger         logger
	rawCollection  *mongo.Collection
	rabbitChannel  *amqp.Channel
	parseQueue     string
	uploadedTopic  string
	kafkaWriter    *kafka.Writer
	requestTimeout time.Duration
	keyring        *cryptox.Keyring
}

type logger interface {
	Info(string, ...any)
	Error(string, ...any)
}

func main() {
	const serviceName = "ingest-service"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)
	mongoURI := env.String("MONGO_URI", "mongodb://localhost:27017")
	mongoDatabase := env.String("MONGO_DATABASE", "finance_os")
	rawCollectionName := env.String("MONGO_RAW_COLLECTION", "raw_imports")
	rabbitURL := env.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	parseQueue := env.String("RABBIT_PARSE_QUEUE", "parse.statement")
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	uploadedTopic := env.String("KAFKA_IMPORT_TOPIC", "statement.uploaded")
	keyring, err := cryptox.NewKeyring(
		env.String("DATA_ENCRYPTION_KEY_ID", "local-v1"),
		env.String("DATA_ENCRYPTION_KEY_B64", ""),
		env.String("DATA_ENCRYPTION_LEGACY_KEYS", ""),
	)
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
	if err := startupx.Retry(startupCtx, logger, "mongodb update raw import indexes", func(ctx context.Context) error {
		_ = mongox.DropIndex(ctx, rawCollection, "import_id_1")
		return mongox.EnsureUniqueCompoundIndex(ctx, rawCollection, "user_id", "import_id")
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
	rabbitChannel, err := startupx.RetryValue(startupCtx, logger, "rabbitmq declare parse queue", func(context.Context) (*amqp.Channel, error) {
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
	defer func() {
		_ = rabbitChannel.Close()
	}()

	if err := startupx.Retry(startupCtx, logger, "kafka broker ping", func(ctx context.Context) error {
		return kafkax.Ping(ctx, kafkaBrokers)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "kafka ensure upload topic", func(ctx context.Context) error {
		return kafkax.EnsureTopic(ctx, kafkaBrokers, uploadedTopic, 1, 1)
	}); err != nil {
		panic(err)
	}
	kafkaWriter := kafkax.NewWriter(kafkaBrokers, uploadedTopic)
	defer func() {
		_ = kafkaWriter.Close()
	}()

	svc := &service{
		logger:         logger,
		rawCollection:  rawCollection,
		rabbitChannel:  rabbitChannel,
		parseQueue:     parseQueue,
		uploadedTopic:  uploadedTopic,
		kafkaWriter:    kafkaWriter,
		requestTimeout: requestTimeout,
		keyring:        keyring,
	}

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)
	mux.HandleFunc("POST /imports/raw", svc.handleImport)
	mux.HandleFunc("GET /imports/{importID}", svc.handleGetImport)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8082"),
		Handler:  mux,
		Logger:   logger,
	}); err != nil {
		panic(err)
	}
}

func (s *service) handleImport(w http.ResponseWriter, r *http.Request) {
	userID, err := userctx.RequireAuthenticatedUserID(r)
	if err != nil {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	payload, filename, err := readImportPayload(r)
	if err != nil {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_import_payload"})
		return
	}
	if len(payload) == 0 {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "empty payload"})
		return
	}

	hash := sha256.Sum256(payload)
	importID := hex.EncodeToString(hash[:])
	now := time.Now().UTC()
	encryptedPayload, encryptedNonce, keyID, err := s.keyring.Encrypt(payload)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "encryption_failed"})
		return
	}
	document := imports.RawImport{
		UserID:     userID,
		ImportID:   importID,
		Filename:   filename,
		SHA256:     importID,
		SizeBytes:  len(payload),
		ContentEnc: encryptedPayload,
		ContentNnc: encryptedNonce,
		ContentKID: keyID,
		Status:     "stored",
		ReceivedAt: now,
		UpdatedAt:  now,
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	filter := userImportFilter(userID, importID)
	result, err := s.rawCollection.UpdateOne(ctx, filter, bson.M{"$setOnInsert": document}, mongooptions.Update().SetUpsert(true))
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "internal_error"})
		return
	}
	if result.UpsertedCount == 0 {
		existing, findErr := s.findImport(ctx, userID, importID)
		if findErr != nil {
			httpx.JSON(w, http.StatusAccepted, map[string]any{"import_id": importID, "already_exists": true})
			return
		}
		requeued, kafkaEmitted, status := s.recoverExistingImport(ctx, existing)
		httpx.JSON(w, http.StatusAccepted, map[string]any{
			"import_id":      existing.ImportID,
			"user_id":        existing.UserID,
			"status":         status,
			"filename":       existing.Filename,
			"already_exists": true,
			"requeued":       requeued,
			"kafka_emitted":  kafkaEmitted,
		})
		return
	}

	job := imports.ParseJob{
		UserID:     userID,
		ImportID:   importID,
		Filename:   filename,
		SHA256:     importID,
		SizeBytes:  len(payload),
		ReceivedAt: now,
	}
	if err := rabbitmq.PublishJSON(ctx, s.rabbitChannel, s.parseQueue, job); err != nil {
		s.markStatus(context.Background(), userID, importID, "queue_failed")
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "queue_publish_failed"})
		return
	}

	kafkaEmitted := true
	event := imports.StatementUploadedEvent{
		UserID:     userID,
		ImportID:   importID,
		Filename:   filename,
		SHA256:     importID,
		SizeBytes:  len(payload),
		Status:     "queued",
		ReceivedAt: now,
	}
	if err := kafkax.PublishJSON(ctx, s.kafkaWriter, importID, event); err != nil {
		kafkaEmitted = false
		s.logger.Error("failed to emit upload event", "import_id", importID, "topic", s.uploadedTopic, "error", err)
	}

	status := "queued"
	if !kafkaEmitted {
		status = "queued_without_event"
	}
	s.markStatus(context.Background(), userID, importID, status)
	s.logger.Info("raw import accepted", "user_id", userID, "import_id", importID, "filename", filename, "queue", s.parseQueue, "topic", s.uploadedTopic, "kafka_emitted", kafkaEmitted)
	httpx.JSON(w, http.StatusAccepted, map[string]any{
		"user_id":         userID,
		"import_id":       importID,
		"filename":        filename,
		"size_bytes":      len(payload),
		"status":          status,
		"rabbit_enqueued": true,
		"kafka_emitted":   kafkaEmitted,
		"received_at":     now,
	})
}

func (s *service) handleGetImport(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	userID, err := userctx.RequireAuthenticatedUserID(r)
	if err != nil {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	item, err := s.findImport(ctx, userID, r.PathValue("importID"))
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
	item.Content = nil
	httpx.JSON(w, http.StatusOK, item)
}

func (s *service) findImport(ctx context.Context, userID, importID string) (imports.RawImport, error) {
	var item imports.RawImport
	err := s.rawCollection.FindOne(ctx, userImportFilter(userID, importID)).Decode(&item)
	return item, err
}

func (s *service) recoverExistingImport(ctx context.Context, existing imports.RawImport) (bool, bool, string) {
	switch existing.Status {
	case "stored", "queue_failed", "queued_without_event":
		requeued := s.enqueueParseJob(ctx, existing)
		kafkaEmitted := s.emitUploadEvent(ctx, existing, "queued")
		status := "queued"
		if !kafkaEmitted {
			status = "queued_without_event"
		}
		if requeued {
			s.markStatus(context.Background(), existing.UserID, existing.ImportID, status)
		}
		return requeued, kafkaEmitted, status
	case "parsed_pending_event":
		requeued := s.enqueueParseJob(ctx, existing)
		if requeued {
			s.logger.Info("requeued existing import to retry parsed event emission", "import_id", existing.ImportID)
		}
		return requeued, false, existing.Status
	default:
		return false, false, existing.Status
	}
}

func (s *service) enqueueParseJob(ctx context.Context, item imports.RawImport) bool {
	job := imports.ParseJob{
		UserID:     item.UserID,
		ImportID:   item.ImportID,
		Filename:   item.Filename,
		SHA256:     item.SHA256,
		SizeBytes:  item.SizeBytes,
		ReceivedAt: item.ReceivedAt,
	}
	if err := rabbitmq.PublishJSON(ctx, s.rabbitChannel, s.parseQueue, job); err != nil {
		s.logger.Error("failed to requeue existing import", "import_id", item.ImportID, "queue", s.parseQueue, "error", err)
		return false
	}
	return true
}

func (s *service) emitUploadEvent(ctx context.Context, item imports.RawImport, status string) bool {
	event := imports.StatementUploadedEvent{
		UserID:     item.UserID,
		ImportID:   item.ImportID,
		Filename:   item.Filename,
		SHA256:     item.SHA256,
		SizeBytes:  item.SizeBytes,
		Status:     status,
		ReceivedAt: item.ReceivedAt,
	}
	if err := kafkax.PublishJSON(ctx, s.kafkaWriter, item.ImportID, event); err != nil {
		s.logger.Error("failed to emit upload event for existing import", "import_id", item.ImportID, "topic", s.uploadedTopic, "error", err)
		return false
	}
	return true
}

func (s *service) markStatus(ctx context.Context, userID, importID, status string) {
	updateCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	_, _ = s.rawCollection.UpdateOne(updateCtx, userImportFilter(userID, importID), bson.M{"$set": bson.M{"status": status, "updated_at": time.Now().UTC()}})
}

func readImportPayload(r *http.Request) ([]byte, string, error) {
	defer r.Body.Close()
	if err := r.ParseMultipartForm(12 << 20); err == nil {
		file, header, err := r.FormFile("file")
		if err == nil {
			defer file.Close()
			payload, readErr := io.ReadAll(io.LimitReader(file, 10<<20))
			return payload, header.Filename, readErr
		}
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		return nil, "", err
	}
	return payload, env.String("IMPORT_FILENAME", "raw-import.bin"), nil
}

func userImportFilter(userID, importID string) bson.M {
	filter := bson.M{"import_id": importID}
	if strings.TrimSpace(userID) != "" {
		filter["user_id"] = strings.TrimSpace(userID)
	}
	return filter
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/startupx"
	"personal-finance-os/internal/platform/ws"
	"personal-finance-os/internal/realtime"
	"personal-finance-os/internal/rules"
)

type service struct {
	logger            *slog.Logger
	hub               *ws.Hub
	presenceStore     realtime.PresenceStore
	transactionReader *kafka.Reader
	alertReader       *kafka.Reader
	requestTimeout    time.Duration
	stateTTL          time.Duration
	redisAddr         string
	redisPrefix       string
	transactionTopic  string
	alertTopic        string
}

func main() {
	const serviceName = "realtime-gateway"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)
	stateTTL := env.Duration("WS_STATE_TTL", 2*time.Minute)
	redisAddr := env.String("REDIS_ADDR", "localhost:6379")
	redisPrefix := env.String("REDIS_PREFIX", "ws")
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	transactionTopic := env.String("KAFKA_TRANSACTION_TOPIC", "transaction.upserted")
	alertTopic := env.String("KAFKA_ALERT_TOPIC", "alert.created")
	transactionGroup := env.String("KAFKA_TRANSACTION_CONSUMER_GROUP", "realtime-gateway-transactions")
	alertGroup := env.String("KAFKA_ALERT_CONSUMER_GROUP", "realtime-gateway-alerts")

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := startupx.Retry(startupCtx, logger, "redis ping", func(ctx context.Context) error {
		return redisClient.Ping(ctx).Err()
	}); err != nil {
		panic(err)
	}
	defer func() {
		_ = redisClient.Close()
	}()

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

	presenceStore := realtime.NewRedisStore(redisClient, redisPrefix, stateTTL)
	hub := ws.NewHub(logger, ws.WithHooks(ws.Hooks{
		OnConnect: func(info ws.ClientInfo) {
			updatePresence(logger, presenceStore, requestTimeout, info)
		},
		OnDisconnect: func(info ws.ClientInfo) {
			removePresence(logger, presenceStore, requestTimeout, info)
		},
		OnSubscriptionsChanged: func(info ws.ClientInfo) {
			updatePresence(logger, presenceStore, requestTimeout, info)
		},
	}))

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
		hub:               hub,
		presenceStore:     presenceStore,
		transactionReader: transactionReader,
		alertReader:       alertReader,
		requestTimeout:    requestTimeout,
		stateTTL:          stateTTL,
		redisAddr:         redisAddr,
		redisPrefix:       redisPrefix,
		transactionTopic:  transactionTopic,
		alertTopic:        alertTopic,
	}

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)
	mux.HandleFunc("GET /ws", hub.ServeWS)
	mux.HandleFunc("GET /api/v1/presence", svc.handlePresence)
	mux.HandleFunc("GET /api/v1/realtime/config", svc.handleConfig)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8088"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			svc.consumeTransactions,
			svc.consumeAlerts,
			svc.refreshPresence,
		},
	}); err != nil {
		panic(err)
	}
}

func (s *service) handlePresence(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	snapshot := s.hub.Snapshot()
	users := summarizeUsers(snapshot)
	response := map[string]any{
		"redis_addr":         s.redisAddr,
		"redis_prefix":       s.redisPrefix,
		"state_ttl":          s.stateTTL.String(),
		"local_connections":  s.hub.Count(),
		"local_active_users": len(users),
		"users":              users,
	}

	if userID := strings.TrimSpace(r.URL.Query().Get("user_id")); userID != "" {
		count, err := s.presenceStore.PresenceCount(ctx, userID)
		if err != nil {
			httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		response["requested_user_id"] = userID
		response["local_user_connections"] = s.hub.CountByUser(userID)
		response["redis_user_connections"] = count
	}

	httpx.JSON(w, http.StatusOK, response)
}

func (s *service) handleConfig(w http.ResponseWriter, _ *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"transaction_topic": s.transactionTopic,
		"alert_topic":       s.alertTopic,
		"redis_addr":        s.redisAddr,
		"redis_prefix":      s.redisPrefix,
		"state_ttl":         s.stateTTL.String(),
		"channels": []string{
			realtime.ChannelDashboard,
			realtime.ChannelTransactions,
			realtime.ChannelAlerts,
		},
	})
}

func (s *service) consumeTransactions(ctx context.Context, logger *slog.Logger) error {
	logger.Info("realtime transaction consumer ready", "topic", s.transactionTopic)
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
	logger.Info("realtime alert consumer ready", "topic", s.alertTopic)
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

	_ = ctx
	envelope := realtime.TransactionEnvelope(event)
	s.broadcastEnvelope(envelope, []string{realtime.ChannelTransactions, realtime.ChannelDashboard})
	return nil
}

func (s *service) handleAlertMessage(ctx context.Context, message kafka.Message) error {
	var alert rules.Alert
	if err := json.Unmarshal(message.Value, &alert); err != nil {
		return err
	}

	_ = ctx
	envelope := realtime.AlertEnvelope(alert)
	s.broadcastEnvelope(envelope, []string{realtime.ChannelAlerts, realtime.ChannelDashboard})
	return nil
}

func (s *service) refreshPresence(ctx context.Context, logger *slog.Logger) error {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
			snapshot := s.hub.Snapshot()
			if err := s.presenceStore.RefreshConnections(operationCtx, snapshot); err != nil {
				logger.Error("presence refresh failed", "error", err)
				cancel()
				continue
			}
			cancel()

			users := summarizeUsers(snapshot)
			s.hub.BroadcastToChannels([]string{realtime.ChannelDashboard}, realtime.PresenceEnvelope(len(snapshot), len(users)))
			logger.Info("presence refreshed", "connections", len(snapshot), "active_users", len(users))
		}
	}
}

func (s *service) broadcastEnvelope(envelope realtime.Envelope, channels []string) {
	if strings.TrimSpace(envelope.UserID) == "" {
		s.hub.BroadcastToChannels(channels, envelope)
		return
	}
	s.hub.BroadcastToUserChannels(envelope.UserID, channels, envelope)
}

func summarizeUsers(snapshot []ws.ClientInfo) []map[string]any {
	counts := make(map[string]int)
	for _, info := range snapshot {
		counts[info.UserID]++
	}

	users := make([]string, 0, len(counts))
	for userID := range counts {
		users = append(users, userID)
	}
	sort.Strings(users)

	result := make([]map[string]any, 0, len(users))
	for _, userID := range users {
		result = append(result, map[string]any{
			"user_id":     userID,
			"connections": counts[userID],
		})
	}
	return result
}

func updatePresence(logger *slog.Logger, store realtime.PresenceStore, timeout time.Duration, info ws.ClientInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := store.UpsertConnection(ctx, info); err != nil {
		logger.Error("presence upsert failed", "connection_id", info.ConnectionID, "user_id", info.UserID, "error", err)
	}
}

func removePresence(logger *slog.Logger, store realtime.PresenceStore, timeout time.Duration, info ws.ClientInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := store.RemoveConnection(ctx, info); err != nil {
		logger.Error("presence removal failed", "connection_id", info.ConnectionID, "user_id", info.UserID, "error", err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/rabbitmq"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/startupx"
	"personal-finance-os/internal/rules"
)

var defaultBudgetLimits = map[string]int64{
	"food":          500,
	"subscriptions": 1500,
	"transport":     3000,
}

type service struct {
	logger         *slog.Logger
	engine         *rules.Engine
	kafkaReader    *kafka.Reader
	kafkaWriter    *kafka.Writer
	rabbitConn     *amqp.Connection
	consumeTopic   string
	publishQueue   string
	alertTopic     string
	requestTimeout time.Duration
	config         rules.Config
}

func main() {
	const serviceName = "rule-engine"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)
	consumeTopic := env.String("KAFKA_RULE_TOPIC", "transaction.upserted")
	alertTopic := env.String("KAFKA_ALERT_TOPIC", "alert.created")
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	consumerGroup := env.String("KAFKA_CONSUMER_GROUP", "rule-engine")
	publishQueue := env.String("RABBIT_NOTIFICATION_QUEUE", "send.telegram")
	rabbitURL := env.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	budgetLimits := rules.ParseBudgetLimits(env.String("RULE_BUDGET_LIMITS", ""), defaultBudgetLimits)

	cfg := rules.Config{
		LargeTransactionThresholdCents:  int64(env.Int("RULE_LARGE_TRANSACTION_THRESHOLD_CENTS", 5000)),
		NewMerchantThresholdCents:       int64(env.Int("RULE_NEW_MERCHANT_THRESHOLD_CENTS", 1000)),
		BudgetWarningRatio:              float64(env.Int("RULE_BUDGET_WARNING_PERCENT", 80)) / 100,
		BudgetCriticalRatio:             float64(env.Int("RULE_BUDGET_CRITICAL_PERCENT", 100)) / 100,
		BudgetLimitsCents:               budgetLimits,
		DefaultChatID:                   env.String("TELEGRAM_CHAT_ID", ""),
		NotifyImportedLargeTransactions: env.Bool("RULE_NOTIFY_IMPORTED_LARGE_TRANSACTIONS", false),
		NotifyImportedNewMerchants:      env.Bool("RULE_NOTIFY_IMPORTED_NEW_MERCHANTS", false),
	}

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	rabbitConn, err := startupx.RetryValue(startupCtx, logger, "rabbitmq connect", func(context.Context) (*amqp.Connection, error) {
		return rabbitmq.Connect(rabbitURL)
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rabbitConn.Close()
	}()
	rabbitChannel, err := startupx.RetryValue(startupCtx, logger, "rabbitmq declare notification queue", func(context.Context) (*amqp.Channel, error) {
		channel, err := rabbitmq.OpenChannel(rabbitConn)
		if err != nil {
			return nil, err
		}
		if err := rabbitmq.DeclareWorkQueue(channel, publishQueue); err != nil {
			_ = channel.Close()
			return nil, err
		}
		return channel, nil
	})
	if err != nil {
		panic(err)
	}
	_ = rabbitChannel.Close()

	if err := startupx.Retry(startupCtx, logger, "kafka broker ping", func(ctx context.Context) error {
		return kafkax.Ping(ctx, kafkaBrokers)
	}); err != nil {
		panic(err)
	}
	if err := startupx.Retry(startupCtx, logger, "kafka ensure alert topic", func(ctx context.Context) error {
		return kafkax.EnsureTopic(ctx, kafkaBrokers, alertTopic, 1, 1)
	}); err != nil {
		panic(err)
	}

	var store rules.StateStore = rules.NewMemoryStore()
	if redisAddr := env.String("REDIS_ADDR", ""); redisAddr != "" {
		client := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := startupx.Retry(startupCtx, logger, "redis ping", func(ctx context.Context) error {
			return client.Ping(ctx).Err()
		}); err != nil {
			panic(err)
		}
		store = rules.NewRedisStore(client, env.String("REDIS_PREFIX", "rules"), env.Duration("RULE_STATE_TTL", 45*24*time.Hour))
		logger.Info("redis-backed rule state configured", "addr", redisAddr)
	}

	engine := rules.NewEngine(cfg, store)
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
	kafkaWriter := kafkax.NewWriter(kafkaBrokers, alertTopic)
	defer func() {
		_ = kafkaWriter.Close()
	}()

	svc := &service{
		logger:         logger,
		engine:         engine,
		kafkaReader:    kafkaReader,
		kafkaWriter:    kafkaWriter,
		rabbitConn:     rabbitConn,
		consumeTopic:   consumeTopic,
		publishQueue:   publishQueue,
		alertTopic:     alertTopic,
		requestTimeout: requestTimeout,
		config:         cfg,
	}

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)
	mux.HandleFunc("GET /api/v1/rules/demo/overspend", func(w http.ResponseWriter, r *http.Request) {
		spent, _ := strconv.Atoi(r.URL.Query().Get("spent"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		httpx.JSON(w, http.StatusOK, map[string]any{
			"spent":       spent,
			"limit":       limit,
			"overspend":   spent > limit,
			"severity":    severity(spent, limit),
			"next_action": "publish notification job",
		})
	})
	mux.HandleFunc("GET /api/v1/rules/config", svc.handleConfig)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8085"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			svc.consumeTransactions,
		},
	}); err != nil {
		panic(err)
	}
}

func (s *service) handleConfig(w http.ResponseWriter, _ *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"large_transaction_threshold_cents":  s.config.LargeTransactionThresholdCents,
		"new_merchant_threshold_cents":       s.config.NewMerchantThresholdCents,
		"budget_warning_ratio":               s.config.BudgetWarningRatio,
		"budget_critical_ratio":              s.config.BudgetCriticalRatio,
		"budget_limits_cents":                s.config.BudgetLimitsCents,
		"notify_imported_large_transactions": s.config.NotifyImportedLargeTransactions,
		"notify_imported_new_merchants":      s.config.NotifyImportedNewMerchants,
		"alert_topic":                        s.alertTopic,
		"notification_queue":                 s.publishQueue,
	})
}

func (s *service) consumeTransactions(ctx context.Context, logger *slog.Logger) error {
	logger.Info("rule engine ready", "consume_topic", s.consumeTopic, "publish_queue", s.publishQueue, "alert_topic", s.alertTopic)
	for {
		message, err := s.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := s.handleTransactionMessage(ctx, message); err != nil {
			return err
		}
		if err := s.kafkaReader.CommitMessages(ctx, message); err != nil {
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

	alerts, err := s.engine.Evaluate(operationCtx, event)
	if err != nil {
		return err
	}
	if len(alerts) == 0 {
		return nil
	}

	channel, err := rabbitmq.OpenChannel(s.rabbitConn)
	if err != nil {
		return err
	}
	defer func() {
		_ = channel.Close()
	}()

	if err := rabbitmq.DeclareWorkQueue(channel, s.publishQueue); err != nil {
		return err
	}

	for _, alert := range alerts {
		job := rules.NewNotificationJob(alert, s.config.DefaultChatID)
		if err := rabbitmq.PublishJSON(operationCtx, channel, s.publishQueue, job); err != nil {
			return err
		}
		if err := kafkax.PublishJSON(operationCtx, s.kafkaWriter, alert.ID, alert); err != nil {
			return err
		}
		s.logger.Info("alert emitted", "alert_id", alert.ID, "type", alert.Type, "severity", alert.Severity, "user_id", alert.UserID)
	}
	return nil
}

func severity(spent, limit int) string {
	if limit <= 0 {
		return "unknown"
	}
	ratio := float64(spent) / float64(limit)
	switch {
	case ratio >= 1.2:
		return "critical"
	case ratio >= 1.0:
		return "warning"
	default:
		return "normal"
	}
}

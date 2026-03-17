package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/rabbitmq"
	"personal-finance-os/internal/platform/runtime"
	"personal-finance-os/internal/platform/startupx"
	"personal-finance-os/internal/rules"
	"personal-finance-os/internal/telegramauth"
)

type service struct {
	logger                 *slog.Logger
	rabbitConn             *amqp.Connection
	queue                  string
	dlq                    string
	requestTimeout         time.Duration
	maxRetries             int
	httpClient             *http.Client
	telegramToken          string
	defaultChatID          string
	telegramAPIBaseURL     string
	telegramPollingEnabled bool
	telegramPollInterval   time.Duration
	telegramDefaultUserID  string
	authServiceURL         string
	ingestServiceURL       string
	parserServiceURL       string
	analyticsServiceURL    string
	ledgerServiceURL       string
	authStore              telegramauth.Store
	allowedTelegramChatIDs map[string]struct{}
	botState               telegramBotState
	pollMu                 sync.Mutex
}

type telegramSendMessageRequest struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

type telegramSendMessageResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
}

type telegramBotState struct {
	mu           sync.RWMutex
	lastUpdateID int64
	lastCommand  string
	lastChatID   string
	lastPollAt   time.Time
	lastError    string
}

func main() {
	const serviceName = "notification-service"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	queue := env.String("RABBIT_NOTIFICATION_QUEUE", "send.telegram")
	dlq := env.String("RABBIT_NOTIFICATION_DLQ", "send.telegram.dlq")
	requestTimeout := env.Duration("REQUEST_TIMEOUT", 10*time.Second)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", 45*time.Second)

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	rabbitConn, err := startupx.RetryValue(startupCtx, logger, "rabbitmq connect", func(context.Context) (*amqp.Connection, error) {
		return rabbitmq.Connect(env.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = rabbitConn.Close()
	}()

	channel, err := startupx.RetryValue(startupCtx, logger, "rabbitmq declare notification queue", func(context.Context) (*amqp.Channel, error) {
		channel, err := rabbitmq.OpenChannel(rabbitConn)
		if err != nil {
			return nil, err
		}
		if err := rabbitmq.DeclareWorkQueue(channel, queue); err != nil {
			_ = channel.Close()
			return nil, err
		}
		return channel, nil
	})
	if err != nil {
		panic(err)
	}
	_ = channel.Close()

	authStore := telegramauth.Store(telegramauth.NewMemoryStore())
	if redisAddr := env.String("REDIS_ADDR", ""); redisAddr != "" {
		client := redis.NewClient(&redis.Options{Addr: redisAddr})
		if err := startupx.Retry(startupCtx, logger, "redis ping", func(ctx context.Context) error {
			return client.Ping(ctx).Err()
		}); err != nil {
			panic(err)
		}
		authStore = telegramauth.NewRedisStore(client, env.String("TELEGRAM_BINDINGS_PREFIX", "telegram:bindings"), env.Duration("TELEGRAM_BINDINGS_TTL", 365*24*time.Hour))
		logger.Info("redis-backed telegram auth store configured", "addr", redisAddr)
	}

	svc := &service{
		logger:         logger,
		rabbitConn:     rabbitConn,
		queue:          queue,
		dlq:            dlq,
		requestTimeout: requestTimeout,
		maxRetries:     env.Int("NOTIFICATION_MAX_RETRIES", 3),
		httpClient: &http.Client{
			Timeout: requestTimeout,
		},
		telegramToken:          env.String("TELEGRAM_BOT_TOKEN", ""),
		defaultChatID:          env.String("TELEGRAM_CHAT_ID", ""),
		telegramAPIBaseURL:     strings.TrimRight(env.String("TELEGRAM_API_BASE_URL", "https://api.telegram.org"), "/"),
		telegramPollingEnabled: env.Bool("TELEGRAM_POLLING_ENABLED", true),
		telegramPollInterval:   env.Duration("TELEGRAM_POLL_INTERVAL", 5*time.Second),
		telegramDefaultUserID:  env.String("TELEGRAM_DEFAULT_USER_ID", "user-demo"),
		authServiceURL:         strings.TrimRight(env.String("AUTH_SERVICE_URL", "http://localhost:8081"), "/"),
		ingestServiceURL:       strings.TrimRight(env.String("INGEST_SERVICE_URL", "http://localhost:8082"), "/"),
		parserServiceURL:       strings.TrimRight(env.String("PARSER_SERVICE_URL", "http://localhost:8083"), "/"),
		analyticsServiceURL:    strings.TrimRight(env.String("ANALYTICS_SERVICE_URL", "http://localhost:8087"), "/"),
		ledgerServiceURL:       strings.TrimRight(env.String("LEDGER_SERVICE_URL", "http://localhost:8084"), "/"),
		authStore:              authStore,
	}
	svc.allowedTelegramChatIDs = parseAllowedChatIDs(env.String("TELEGRAM_ALLOWED_CHAT_IDS", ""), svc.defaultChatID)

	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)
	mux.HandleFunc("GET /api/v1/notifications/status", svc.handleStatus)
	mux.HandleFunc("POST /api/v1/notifications/telegram/demo", svc.handleDemo)
	mux.HandleFunc("POST /api/v1/notifications/telegram/poll/once", svc.handleTelegramPollOnce)

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8086"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			svc.consumeNotifications,
			svc.pollTelegramUpdates,
		},
	}); err != nil {
		panic(err)
	}
}

func (s *service) handleStatus(w http.ResponseWriter, _ *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"queue":                    s.queue,
		"dlq":                      s.dlq,
		"max_retries":              s.maxRetries,
		"telegram_configured":      strings.TrimSpace(s.telegramToken) != "",
		"default_chat_id_set":      strings.TrimSpace(s.defaultChatID) != "",
		"telegram_polling_enabled": s.telegramPollingEnabled,
		"telegram_poll_interval":   s.telegramPollInterval.String(),
		"telegram_allowed_chats":   len(s.allowedTelegramChatIDs),
		"telegram_default_user_id": s.telegramDefaultUserID,
		"auth_service_url":         s.authServiceURL,
		"ingest_service_url":       s.ingestServiceURL,
		"parser_service_url":       s.parserServiceURL,
		"analytics_service_url":    s.analyticsServiceURL,
		"ledger_service_url":       s.ledgerServiceURL,
		"bot_state":                s.botState.snapshot(),
	})
}

func (s *service) handleDemo(w http.ResponseWriter, r *http.Request) {
	channel, err := rabbitmq.OpenChannel(s.rabbitConn)
	if err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer func() {
		_ = channel.Close()
	}()

	job := rules.NewNotificationJob(rules.Alert{
		ID:        "alert-demo",
		UserID:    "user-demo",
		Type:      "demo",
		Severity:  "info",
		Message:   "Demo notification from notification-service",
		CreatedAt: time.Now().UTC(),
	}, s.defaultChatID)
	if err := rabbitmq.PublishJSON(r.Context(), channel, s.queue, job); err != nil {
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpx.JSON(w, http.StatusAccepted, map[string]any{"status": "queued", "queue": s.queue})
}

func (s *service) handleTelegramPollOnce(w http.ResponseWriter, r *http.Request) {
	processed, err := s.pollTelegramOnce(r.Context())
	if err != nil {
		httpx.JSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{
		"processed_updates": processed,
		"bot_state":         s.botState.snapshot(),
	})
}

func (s *service) consumeNotifications(ctx context.Context, logger *slog.Logger) error {
	channel, err := rabbitmq.OpenChannel(s.rabbitConn)
	if err != nil {
		return err
	}
	defer func() {
		_ = channel.Close()
	}()
	if err := rabbitmq.DeclareWorkQueue(channel, s.queue); err != nil {
		return err
	}
	if err := channel.Qos(1, 0, false); err != nil {
		return err
	}

	messages, err := channel.Consume(s.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	logger.Info("notification worker ready", "queue", s.queue, "dlq", s.dlq, "max_retries", s.maxRetries)

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-messages:
			if !ok {
				return errors.New("rabbitmq delivery channel closed")
			}
			if err := s.handleDelivery(ctx, channel, delivery); err != nil {
				logger.Error("notification delivery failed", "error", err)
				_ = delivery.Nack(false, false)
				continue
			}
			_ = delivery.Ack(false)
		}
	}
}

func (s *service) handleDelivery(ctx context.Context, channel *amqp.Channel, delivery amqp.Delivery) error {
	var job rules.NotificationJob
	if err := json.Unmarshal(delivery.Body, &job); err != nil {
		return err
	}

	operationCtx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()

	err := s.deliverTelegram(operationCtx, job)
	if err == nil {
		s.logger.Info("notification delivered", "alert_id", job.Alert.ID, "attempt", job.Attempt, "channel", job.Channel, "dry_run", job.IsDryRun)
		return nil
	}

	if job.Attempt >= s.maxRetries {
		s.logger.Error("notification exhausted retries", "alert_id", job.Alert.ID, "attempt", job.Attempt, "error", err)
		return err
	}

	job.Attempt++
	job.LastError = err.Error()
	if err := rabbitmq.PublishJSON(operationCtx, channel, s.queue, job); err != nil {
		return err
	}
	s.logger.Warn("notification requeued", "alert_id", job.Alert.ID, "next_attempt", job.Attempt, "error", err)
	return nil
}

func (s *service) deliverTelegram(ctx context.Context, job rules.NotificationJob) error {
	chatID := strings.TrimSpace(job.ChatID)
	if chatID == "" {
		chatID = strings.TrimSpace(s.defaultChatID)
	}

	if strings.TrimSpace(s.telegramToken) == "" || chatID == "" {
		s.logger.Info("notification dry run", "alert_id", job.Alert.ID, "chat_id_set", chatID != "", "token_set", strings.TrimSpace(s.telegramToken) != "")
		return nil
	}

	payload := telegramSendMessageRequest{
		ChatID: chatID,
		Text:   job.Alert.Message,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/bot%s/sendMessage", s.telegramAPIBaseURL, s.telegramToken)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := s.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	var telegramResponse telegramSendMessageResponse
	if err := json.NewDecoder(response.Body).Decode(&telegramResponse); err != nil {
		return err
	}
	if response.StatusCode >= 300 || !telegramResponse.OK {
		return fmt.Errorf("telegram send failed: status=%d description=%s", response.StatusCode, telegramResponse.Description)
	}
	return nil
}

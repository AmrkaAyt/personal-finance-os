package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
)

func main() {
	const serviceName = "notification-service"

	logger := logging.New(serviceName)
	telegramQueue := env.String("RABBIT_NOTIFICATION_QUEUE", "send.telegram")
	dlq := env.String("RABBIT_NOTIFICATION_DLQ", "send.telegram.dlq")
	telegramBotTokenSet := env.String("TELEGRAM_BOT_TOKEN", "") != ""
	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)

	mux.HandleFunc("POST /api/v1/notifications/telegram/demo", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusAccepted, map[string]any{
			"status":              "queued",
			"telegram_queue":      telegramQueue,
			"dlq":                 dlq,
			"telegram_configured": telegramBotTokenSet,
		})
	})

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8086"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			func(ctx context.Context, logger *slog.Logger) error {
				logger.Info("notification worker ready", "queue", telegramQueue, "dlq", dlq)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Millisecond):
					return nil
				}
			},
		},
	}); err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
)

func main() {
	const serviceName = "rule-engine"

	logger := logging.New(serviceName)
	consumeTopic := env.String("KAFKA_RULE_TOPIC", "transaction.upserted")
	publishQueue := env.String("RABBIT_NOTIFICATION_QUEUE", "send.telegram")
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

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8085"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			func(ctx context.Context, logger *slog.Logger) error {
				logger.Info("rule engine ready", "consume_topic", consumeTopic, "publish_queue", publishQueue)
				<-ctx.Done()
				return nil
			},
		},
	}); err != nil {
		panic(err)
	}
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

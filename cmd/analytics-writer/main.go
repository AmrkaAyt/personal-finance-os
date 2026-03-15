package main

import (
	"context"
	"log/slog"
	"net/http"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/runtime"
)

func main() {
	const serviceName = "analytics-writer"

	logger := logging.New(serviceName)
	consumeTopic := env.String("KAFKA_ANALYTICS_TOPIC", "transaction.upserted")
	clickhouseDSN := env.String("CLICKHOUSE_DSN", "clickhouse://localhost:9000")
	mux := http.NewServeMux()
	httpx.RegisterBaseRoutes(mux, serviceName)

	mux.HandleFunc("GET /api/v1/analytics/projections", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]any{
			"source_topic": consumeTopic,
			"target":       clickhouseDSN,
			"projection":   "daily_spend_by_category",
		})
	})

	if err := runtime.Run(runtime.Config{
		Name:     serviceName,
		HTTPAddr: env.String("HTTP_ADDR", ":8087"),
		Handler:  mux,
		Logger:   logger,
		Background: []runtime.BackgroundFunc{
			func(ctx context.Context, logger *slog.Logger) error {
				logger.Info("analytics writer ready", "consume_topic", consumeTopic, "clickhouse_dsn", clickhouseDSN)
				<-ctx.Done()
				return nil
			},
		},
	}); err != nil {
		panic(err)
	}
}

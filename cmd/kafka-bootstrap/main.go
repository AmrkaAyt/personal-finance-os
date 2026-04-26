package main

import (
	"context"
	"sort"
	"strings"
	"time"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/startupx"
)

func main() {
	const serviceName = "kafka-bootstrap"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	startupTimeout := env.Duration("STARTUP_TIMEOUT", time.Minute)
	kafkaBrokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	partitions := env.Int("KAFKA_TOPIC_PARTITIONS", 1)
	replicationFactor := env.Int("KAFKA_TOPIC_REPLICATION_FACTOR", 1)

	startupCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	if err := startupx.Retry(startupCtx, logger, "kafka broker ping for bootstrap", func(ctx context.Context) error {
		return kafkax.Ping(ctx, kafkaBrokers)
	}); err != nil {
		panic(err)
	}

	topics := collectTopics()
	for _, topic := range topics {
		if err := startupx.Retry(startupCtx, logger, "kafka ensure topic "+topic, func(ctx context.Context) error {
			return kafkax.EnsureTopic(ctx, kafkaBrokers, topic, partitions, replicationFactor)
		}); err != nil {
			panic(err)
		}
		logger.Info("kafka topic ensured", "topic", topic, "partitions", partitions, "replication_factor", replicationFactor)
	}

	logger.Info("kafka bootstrap completed", "topics", topics)
}

func collectTopics() []string {
	seen := make(map[string]struct{})
	topics := make([]string, 0, 6)

	add := func(value string) {
		topic := strings.TrimSpace(value)
		if topic == "" {
			return
		}
		if _, exists := seen[topic]; exists {
			return
		}
		seen[topic] = struct{}{}
		topics = append(topics, topic)
	}

	add(env.String("KAFKA_IMPORT_TOPIC", "statement.uploaded"))
	add(env.String("KAFKA_PARSED_TOPIC", "statement.parsed"))
	add(env.String("KAFKA_TRANSACTION_TOPIC", "transaction.upserted"))
	add(env.String("KAFKA_ALERT_TOPIC", "alert.created"))
	add(env.String("KAFKA_ANALYTICS_TOPIC", "transaction.upserted"))
	add(env.String("KAFKA_QUARANTINE_TOPIC", "event.quarantine"))

	sort.Strings(topics)
	return topics
}

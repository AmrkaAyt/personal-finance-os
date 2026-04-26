package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"personal-finance-os/internal/platform/env"
	"personal-finance-os/internal/platform/kafkax"
	"personal-finance-os/internal/platform/logging"
	"personal-finance-os/internal/platform/startupx"
)

type quarantineFilter struct {
	ID          string
	Service     string
	SourceTopic string
	ErrorKind   string
}

type quarantineSummary struct {
	Total           int               `json:"total"`
	Replayable      int               `json:"replayable"`
	ByService       map[string]int    `json:"by_service"`
	BySourceTopic   map[string]int    `json:"by_source_topic"`
	ByErrorKind     map[string]int    `json:"by_error_kind"`
	OldestEventTime time.Time         `json:"oldest_event_time,omitempty"`
	NewestEventTime time.Time         `json:"newest_event_time,omitempty"`
	Sample          []summaryEnvelope `json:"sample"`
}

type summaryEnvelope struct {
	ID            string    `json:"id"`
	Service       string    `json:"service"`
	SourceTopic   string    `json:"source_topic"`
	ErrorKind     string    `json:"error_kind"`
	PayloadSize   int       `json:"payload_size"`
	Replayable    bool      `json:"replayable"`
	QuarantinedAt time.Time `json:"quarantined_at"`
}

func main() {
	const serviceName = "quarantine-operator"

	env.LoadService(serviceName)
	logger := logging.New(serviceName)
	action := strings.ToLower(env.String("QUARANTINE_ACTION", "summary"))
	brokers := env.Strings("KAFKA_BROKERS", []string{"localhost:9092"})
	topic := env.String("KAFKA_QUARANTINE_TOPIC", "event.quarantine")
	replayTopicOverride := env.String("QUARANTINE_REPLAY_TOPIC_OVERRIDE", "")
	scanTimeout := env.Duration("QUARANTINE_SCAN_TIMEOUT", 5*time.Second)
	limit := env.Int("QUARANTINE_LIMIT", 100)
	dryRun := env.Bool("QUARANTINE_DRY_RUN", true)
	replayApproved := env.Bool("QUARANTINE_REPLAY_APPROVED", false)
	replayReason := strings.TrimSpace(env.String("QUARANTINE_REPLAY_REASON", ""))
	replayOperator := strings.TrimSpace(env.String("QUARANTINE_REPLAY_OPERATOR", "local-operator"))
	filter := quarantineFilter{
		ID:          strings.TrimSpace(env.String("QUARANTINE_FILTER_ID", "")),
		Service:     strings.TrimSpace(env.String("QUARANTINE_FILTER_SERVICE", "")),
		SourceTopic: strings.TrimSpace(env.String("QUARANTINE_FILTER_SOURCE_TOPIC", "")),
		ErrorKind:   strings.TrimSpace(env.String("QUARANTINE_FILTER_ERROR_KIND", "")),
	}

	ctx, cancel := context.WithTimeout(context.Background(), scanTimeout+15*time.Second)
	defer cancel()

	if err := startupx.Retry(ctx, logger, "kafka ping", func(ctx context.Context) error {
		return kafkax.Ping(ctx, brokers)
	}); err != nil {
		panic(err)
	}

	operator := &service{
		action:              action,
		logger:              logger,
		brokers:             brokers,
		topic:               topic,
		replayTopicOverride: replayTopicOverride,
		scanTimeout:         scanTimeout,
		limit:               limit,
		dryRun:              dryRun,
		replayApproved:      replayApproved,
		replayReason:        replayReason,
		replayOperator:      replayOperator,
		filter:              filter,
	}
	if err := operator.run(ctx); err != nil {
		panic(err)
	}
}

type service struct {
	action              string
	logger              logger
	brokers             []string
	topic               string
	replayTopicOverride string
	scanTimeout         time.Duration
	limit               int
	dryRun              bool
	replayApproved      bool
	replayReason        string
	replayOperator      string
	filter              quarantineFilter
}

type logger interface {
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
}

func (s *service) run(ctx context.Context) error {
	events, err := s.readEvents(ctx)
	if err != nil {
		return err
	}

	switch s.action {
	case "summary":
		return printJSON(buildSummary(events))
	case "list":
		return printJSON(events)
	case "replay":
		return s.replay(ctx, events)
	default:
		return fmt.Errorf("unsupported QUARANTINE_ACTION %q", s.action)
	}
}

func (s *service) readEvents(ctx context.Context) ([]kafkax.QuarantineEvent, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     s.brokers,
		Topic:       s.topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     time.Second,
	})
	defer func() {
		_ = reader.Close()
	}()

	scanCtx, cancel := context.WithTimeout(ctx, s.scanTimeout)
	defer cancel()

	events := make([]kafkax.QuarantineEvent, 0, max(1, s.limit))
	for {
		if s.limit > 0 && len(events) >= s.limit {
			return events, nil
		}

		message, err := reader.ReadMessage(scanCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return events, nil
			}
			return nil, err
		}

		var event kafkax.QuarantineEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			s.logger.Warn("skipping invalid quarantine event payload", "topic", message.Topic, "offset", message.Offset, "error", err)
			continue
		}
		if !matchesFilter(event, s.filter) {
			continue
		}
		events = append(events, event)
	}
}

func (s *service) replay(ctx context.Context, events []kafkax.QuarantineEvent) error {
	if len(events) == 0 {
		return printJSON(map[string]any{
			"action": "replay",
			"status": "no_matching_events",
			"count":  0,
		})
	}

	if s.dryRun {
		replayable := make([]summaryEnvelope, 0, len(events))
		for _, event := range events {
			replayable = append(replayable, summaryEnvelope{
				ID:            event.ID,
				Service:       event.Service,
				SourceTopic:   effectiveReplayTopic(event, s.replayTopicOverride),
				ErrorKind:     event.ErrorKind,
				PayloadSize:   event.PayloadSize,
				Replayable:    strings.TrimSpace(event.PayloadB64) != "",
				QuarantinedAt: event.QuarantinedAt,
			})
		}
		return printJSON(map[string]any{
			"action":             "replay",
			"dry_run":            true,
			"matching_count":     len(events),
			"commit_requires":    "QUARANTINE_DRY_RUN=false, QUARANTINE_REPLAY_APPROVED=true, and non-empty QUARANTINE_REPLAY_REASON",
			"recommended_filter": recommendedReplayFilter(events),
			"events":             replayable,
		})
	}
	if err := s.validateReplayApproval(len(events)); err != nil {
		return err
	}

	replayed := 0
	skipped := 0
	writerCache := make(map[string]*kafka.Writer)
	defer func() {
		for _, writer := range writerCache {
			_ = writer.Close()
		}
	}()

	for _, event := range events {
		message, topic, err := buildReplayMessage(event, s.replayTopicOverride, replayAudit{
			Operator:   s.replayOperator,
			Reason:     s.replayReason,
			ApprovedAt: time.Now().UTC(),
		})
		if err != nil {
			skipped++
			s.logger.Warn("skipping quarantine replay event", "id", event.ID, "error", err)
			continue
		}
		writer := writerCache[topic]
		if writer == nil {
			writer = kafkax.NewWriter(s.brokers, topic)
			writerCache[topic] = writer
		}
		if err := writer.WriteMessages(ctx, message); err != nil {
			return err
		}
		replayed++
	}

	return printJSON(map[string]any{
		"action":            "replay",
		"dry_run":           false,
		"approved":          s.replayApproved,
		"approval_operator": s.replayOperator,
		"approval_reason":   s.replayReason,
		"matched":           len(events),
		"replayed":          replayed,
		"skipped":           skipped,
	})
}

func (s *service) validateReplayApproval(eventCount int) error {
	if eventCount <= 0 {
		return nil
	}
	if !s.replayApproved {
		return fmt.Errorf("replay commit requires QUARANTINE_REPLAY_APPROVED=true")
	}
	if strings.TrimSpace(s.replayReason) == "" {
		return fmt.Errorf("replay commit requires non-empty QUARANTINE_REPLAY_REASON")
	}
	if strings.TrimSpace(s.replayOperator) == "" {
		return fmt.Errorf("replay commit requires non-empty QUARANTINE_REPLAY_OPERATOR")
	}
	return nil
}

func matchesFilter(event kafkax.QuarantineEvent, filter quarantineFilter) bool {
	if filter.ID != "" && strings.TrimSpace(event.ID) != filter.ID {
		return false
	}
	if filter.Service != "" && strings.TrimSpace(event.Service) != filter.Service {
		return false
	}
	if filter.SourceTopic != "" && strings.TrimSpace(event.SourceTopic) != filter.SourceTopic {
		return false
	}
	if filter.ErrorKind != "" && strings.TrimSpace(event.ErrorKind) != filter.ErrorKind {
		return false
	}
	return true
}

func buildSummary(events []kafkax.QuarantineEvent) quarantineSummary {
	summary := quarantineSummary{
		ByService:     map[string]int{},
		BySourceTopic: map[string]int{},
		ByErrorKind:   map[string]int{},
		Sample:        make([]summaryEnvelope, 0, min(len(events), 10)),
	}
	for _, event := range events {
		summary.Total++
		if strings.TrimSpace(event.PayloadB64) != "" {
			summary.Replayable++
		}
		summary.ByService[event.Service]++
		summary.BySourceTopic[event.SourceTopic]++
		summary.ByErrorKind[event.ErrorKind]++
		if summary.OldestEventTime.IsZero() || event.QuarantinedAt.Before(summary.OldestEventTime) {
			summary.OldestEventTime = event.QuarantinedAt
		}
		if summary.NewestEventTime.IsZero() || event.QuarantinedAt.After(summary.NewestEventTime) {
			summary.NewestEventTime = event.QuarantinedAt
		}
		if len(summary.Sample) < 10 {
			summary.Sample = append(summary.Sample, summaryEnvelope{
				ID:            event.ID,
				Service:       event.Service,
				SourceTopic:   event.SourceTopic,
				ErrorKind:     event.ErrorKind,
				PayloadSize:   event.PayloadSize,
				Replayable:    strings.TrimSpace(event.PayloadB64) != "",
				QuarantinedAt: event.QuarantinedAt,
			})
		}
	}
	sort.Slice(summary.Sample, func(i, j int) bool {
		return summary.Sample[i].QuarantinedAt.After(summary.Sample[j].QuarantinedAt)
	})
	return summary
}

type replayAudit struct {
	Operator   string
	Reason     string
	ApprovedAt time.Time
}

func buildReplayMessage(event kafkax.QuarantineEvent, topicOverride string, audit replayAudit) (kafka.Message, string, error) {
	topic := effectiveReplayTopic(event, topicOverride)
	if topic == "" {
		return kafka.Message{}, "", fmt.Errorf("missing source topic")
	}
	payloadB64 := strings.TrimSpace(event.PayloadB64)
	if payloadB64 == "" {
		return kafka.Message{}, "", fmt.Errorf("quarantine event %s does not contain payload", event.ID)
	}
	payload, err := base64.StdEncoding.DecodeString(payloadB64)
	if err != nil {
		return kafka.Message{}, "", err
	}

	headers := []kafka.Header{
		{Key: "x-quarantine-replay-id", Value: []byte(strings.TrimSpace(event.ID))},
		{Key: "x-quarantine-replayed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		{Key: "x-quarantine-replay-operator", Value: []byte(strings.TrimSpace(audit.Operator))},
		{Key: "x-quarantine-replay-reason", Value: []byte(strings.TrimSpace(audit.Reason))},
	}
	if !audit.ApprovedAt.IsZero() {
		headers = append(headers, kafka.Header{Key: "x-quarantine-replay-approved-at", Value: []byte(audit.ApprovedAt.UTC().Format(time.RFC3339Nano))})
	}
	return kafka.Message{
		Topic:   topic,
		Key:     []byte(strings.TrimSpace(event.MessageKey)),
		Value:   payload,
		Headers: headers,
		Time:    time.Now().UTC(),
	}, topic, nil
}

func effectiveReplayTopic(event kafkax.QuarantineEvent, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.TrimSpace(override)
	}
	return strings.TrimSpace(event.SourceTopic)
}

func recommendedReplayFilter(events []kafkax.QuarantineEvent) string {
	if len(events) == 1 {
		return "QUARANTINE_FILTER_ID=" + events[0].ID
	}
	return "narrow by QUARANTINE_FILTER_SERVICE, QUARANTINE_FILTER_SOURCE_TOPIC, QUARANTINE_FILTER_ERROR_KIND, or QUARANTINE_FILTER_ID before commit"
}

func printJSON(payload any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(payload)
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}

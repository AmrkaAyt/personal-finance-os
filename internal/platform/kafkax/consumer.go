package kafkax

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type Handler func(context.Context, kafka.Message) error

type ErrorKind string

const (
	ErrorKindPermanent ErrorKind = "permanent"
	ErrorKindTransient ErrorKind = "transient"
)

type ProcessingError struct {
	Kind ErrorKind
	Err  error
}

func (e *ProcessingError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *ProcessingError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &ProcessingError{Kind: ErrorKindPermanent, Err: err}
}

func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &ProcessingError{Kind: ErrorKindTransient, Err: err}
}

func IsPermanent(err error) bool {
	var processingErr *ProcessingError
	if errors.As(err, &processingErr) {
		return processingErr.Kind == ErrorKindPermanent
	}
	return false
}

type QuarantineHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type QuarantineEvent struct {
	ID              string             `json:"id"`
	Service         string             `json:"service"`
	ConsumerGroup   string             `json:"consumer_group,omitempty"`
	SourceTopic     string             `json:"source_topic"`
	SourcePartition int                `json:"source_partition"`
	SourceOffset    int64              `json:"source_offset"`
	MessageKey      string             `json:"message_key,omitempty"`
	ErrorKind       string             `json:"error_kind"`
	Error           string             `json:"error"`
	PayloadSHA256   string             `json:"payload_sha256"`
	PayloadSize     int                `json:"payload_size"`
	PayloadB64      string             `json:"payload_b64,omitempty"`
	Headers         []QuarantineHeader `json:"headers,omitempty"`
	QuarantinedAt   time.Time          `json:"quarantined_at"`
}

type ConsumerOptions struct {
	Name             string
	Reader           Reader
	QuarantineWriter Writer
	Handler          Handler
	Logger           *slog.Logger
	ConsumerGroup    string
	RetryBackoff     time.Duration
	MaxAttempts      int
	IncludePayload   bool
}

func ConsumeLoop(ctx context.Context, options ConsumerOptions) error {
	if options.Reader == nil {
		return errors.New("kafka consumer reader is required")
	}
	if options.Handler == nil {
		return errors.New("kafka consumer handler is required")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Name == "" {
		options.Name = "kafka-consumer"
	}
	if options.RetryBackoff <= 0 {
		options.RetryBackoff = 2 * time.Second
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}

	for {
		message, err := options.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			options.Logger.Error("kafka fetch failed", "consumer", options.Name, "error", err)
			if !sleepWithContext(ctx, options.RetryBackoff) {
				return nil
			}
			continue
		}

		if err := consumeMessage(ctx, options, message); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			options.Logger.Error("kafka consumer message loop failed", "consumer", options.Name, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
			if !sleepWithContext(ctx, options.RetryBackoff) {
				return nil
			}
		}
	}
}

func consumeMessage(ctx context.Context, options ConsumerOptions, message kafka.Message) error {
	attempt := 0

	for {
		attempt++
		err := options.Handler(ctx, message)
		if err == nil {
			return commitWithRetry(ctx, options, message)
		}
		if IsPermanent(err) {
			return quarantineAndCommit(ctx, options, message, err)
		}

		options.Logger.Warn("kafka message processing failed; retrying", "consumer", options.Name, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "attempt", attempt, "max_attempts", options.MaxAttempts, "error", err)
		if attempt >= options.MaxAttempts {
			options.Logger.Error("kafka message reached max attempts; keeping message in retry loop", "consumer", options.Name, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "attempts", attempt, "error", err)
			attempt = 0
		}
		if !sleepWithContext(ctx, options.RetryBackoff) {
			return ctx.Err()
		}
	}
}

func commitWithRetry(ctx context.Context, options ConsumerOptions, message kafka.Message) error {
	for {
		if err := options.Reader.CommitMessages(ctx, message); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			options.Logger.Error("kafka commit failed; retrying", "consumer", options.Name, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
			if !sleepWithContext(ctx, options.RetryBackoff) {
				return ctx.Err()
			}
			continue
		}
		return nil
	}
}

func quarantineAndCommit(ctx context.Context, options ConsumerOptions, message kafka.Message, cause error) error {
	quarantineEvent := NewQuarantineEvent(options.Name, options.ConsumerGroup, message, cause, options.IncludePayload)

	if options.QuarantineWriter != nil {
		for {
			body, marshalErr := json.Marshal(quarantineEvent)
			if marshalErr != nil {
				return marshalErr
			}
			if err := options.QuarantineWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(quarantineEvent.ID),
				Value: body,
				Time:  time.Now().UTC(),
			}); err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return nil
				}
				options.Logger.Error("kafka quarantine publish failed; retrying", "consumer", options.Name, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
				if !sleepWithContext(ctx, options.RetryBackoff) {
					return ctx.Err()
				}
				continue
			}
			break
		}
	}

	options.Logger.Warn("kafka message quarantined", "consumer", options.Name, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", quarantineEvent.Error, "error_kind", quarantineEvent.ErrorKind)
	return commitWithRetry(ctx, options, message)
}

func NewQuarantineEvent(service, consumerGroup string, message kafka.Message, cause error, includePayload bool) QuarantineEvent {
	payloadSum := sha256.Sum256(message.Value)
	headers := make([]QuarantineHeader, 0, len(message.Headers))
	for _, header := range message.Headers {
		headers = append(headers, QuarantineHeader{
			Key:   header.Key,
			Value: base64.StdEncoding.EncodeToString(header.Value),
		})
	}

	event := QuarantineEvent{
		ID:              fmt.Sprintf("%s:%s:%d:%d", service, message.Topic, message.Partition, message.Offset),
		Service:         service,
		ConsumerGroup:   consumerGroup,
		SourceTopic:     message.Topic,
		SourcePartition: message.Partition,
		SourceOffset:    message.Offset,
		MessageKey:      string(message.Key),
		ErrorKind:       string(errorKindOf(cause)),
		Error:           cause.Error(),
		PayloadSHA256:   hex.EncodeToString(payloadSum[:]),
		PayloadSize:     len(message.Value),
		Headers:         headers,
		QuarantinedAt:   time.Now().UTC(),
	}
	if includePayload && len(message.Value) > 0 {
		event.PayloadB64 = base64.StdEncoding.EncodeToString(message.Value)
	}
	return event
}

func errorKindOf(err error) ErrorKind {
	var processingErr *ProcessingError
	if errors.As(err, &processingErr) {
		return processingErr.Kind
	}
	return ErrorKindTransient
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		delay = time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func QuarantineMessage(topic string, event QuarantineEvent) kafka.Message {
	body, _ := json.Marshal(event)
	return kafka.Message{
		Topic: topic,
		Key:   []byte(event.ID),
		Value: body,
		Time:  time.Now().UTC(),
	}
}

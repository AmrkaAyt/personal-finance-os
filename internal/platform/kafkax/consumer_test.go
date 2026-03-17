package kafkax

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeReader struct {
	mu        sync.Mutex
	messages  []kafka.Message
	fetches   int
	committed []kafka.Message
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fetches >= len(r.messages) {
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	message := r.messages[r.fetches]
	r.fetches++
	return message, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.committed = append(r.committed, messages...)
	return nil
}

type fakeWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.messages = append(w.messages, messages...)
	return nil
}

func TestConsumeLoopQuarantinesPermanentMessageAndContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &fakeReader{
		messages: []kafka.Message{
			{Topic: "transaction.upserted", Partition: 0, Offset: 1, Key: []byte("bad"), Value: []byte(`{`)},
			{Topic: "transaction.upserted", Partition: 0, Offset: 2, Key: []byte("good"), Value: []byte(`ok`)},
		},
	}
	writer := &fakeWriter{}
	var processed int

	errCh := make(chan error, 1)
	go func() {
		errCh <- ConsumeLoop(ctx, ConsumerOptions{
			Name:             "test-consumer",
			Reader:           reader,
			QuarantineWriter: writer,
			Handler: func(_ context.Context, message kafka.Message) error {
				if string(message.Value) == "ok" {
					processed++
					cancel()
					return nil
				}
				return Permanent(errors.New("bad payload"))
			},
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			RetryBackoff: 5 * time.Millisecond,
			MaxAttempts:  2,
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ConsumeLoop returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeLoop did not finish in time")
	}

	if processed != 1 {
		t.Fatalf("expected second message to be processed, got %d", processed)
	}
	if len(reader.committed) != 2 {
		t.Fatalf("expected 2 committed messages, got %d", len(reader.committed))
	}
	if len(writer.messages) != 1 {
		t.Fatalf("expected 1 quarantined message, got %d", len(writer.messages))
	}
}

func TestConsumeLoopRetriesTransientMessageUntilSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := &fakeReader{
		messages: []kafka.Message{
			{Topic: "transaction.upserted", Partition: 0, Offset: 10, Key: []byte("retry"), Value: []byte(`ok`)},
		},
	}
	writer := &fakeWriter{}
	attempts := 0

	errCh := make(chan error, 1)
	go func() {
		errCh <- ConsumeLoop(ctx, ConsumerOptions{
			Name:             "test-consumer",
			Reader:           reader,
			QuarantineWriter: writer,
			Handler: func(_ context.Context, message kafka.Message) error {
				_ = message
				attempts++
				if attempts < 3 {
					return errors.New("temporary downstream failure")
				}
				cancel()
				return nil
			},
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			RetryBackoff: 5 * time.Millisecond,
			MaxAttempts:  2,
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ConsumeLoop returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeLoop did not finish in time")
	}

	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if len(reader.committed) != 1 {
		t.Fatalf("expected 1 committed message, got %d", len(reader.committed))
	}
	if len(writer.messages) != 0 {
		t.Fatalf("expected no quarantined messages, got %d", len(writer.messages))
	}
}

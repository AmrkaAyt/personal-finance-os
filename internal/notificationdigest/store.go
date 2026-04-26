package notificationdigest

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"personal-finance-os/internal/rules"
)

type Item struct {
	AlertID        string    `json:"alert_id"`
	Type           string    `json:"type"`
	Severity       string    `json:"severity"`
	Message        string    `json:"message"`
	Category       string    `json:"category,omitempty"`
	Merchant       string    `json:"merchant,omitempty"`
	AmountCents    int64     `json:"amount_cents,omitempty"`
	SourceImportID string    `json:"source_import_id,omitempty"`
	TransactionID  string    `json:"transaction_id,omitempty"`
	NotificationAt time.Time `json:"notification_at"`
}

type Digest struct {
	Key            string         `json:"key"`
	UserID         string         `json:"user_id"`
	ChatID         string         `json:"chat_id"`
	Channel        string         `json:"channel"`
	SourceImportID string         `json:"source_import_id,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	DueAt          time.Time      `json:"due_at"`
	LastUpdateAt   time.Time      `json:"last_update_at"`
	Items          []Item         `json:"items"`
	Counts         map[string]int `json:"counts"`
}

type Store interface {
	Append(ctx context.Context, key string, job rules.NotificationJob, dueAt time.Time, maxItems int) (Digest, error)
	DueKeys(ctx context.Context, now time.Time, limit int64) ([]string, error)
	Take(ctx context.Context, key string) (Digest, bool, error)
	PendingCount(ctx context.Context) (int64, error)
}

type MemoryStore struct {
	mu      sync.Mutex
	digests map[string]Digest
}

type RedisStore struct {
	client        *redis.Client
	payloadPrefix string
	dueKey        string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		digests: make(map[string]Digest),
	}
}

func NewRedisStore(client *redis.Client, prefix string) *RedisStore {
	if strings.TrimSpace(prefix) == "" {
		prefix = "notification:digest"
	}
	return &RedisStore{
		client:        client,
		payloadPrefix: strings.TrimSpace(prefix) + ":payload",
		dueKey:        strings.TrimSpace(prefix) + ":due",
	}
}

func (s *MemoryStore) Append(_ context.Context, key string, job rules.NotificationJob, dueAt time.Time, maxItems int) (Digest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	digest := s.digests[key]
	now := time.Now().UTC()
	if digest.Key == "" {
		digest = newDigest(key, job, dueAt, now)
	}
	digest = appendItem(digest, job, maxItems, now)
	s.digests[key] = digest
	return digest, nil
}

func (s *MemoryStore) DueKeys(_ context.Context, now time.Time, limit int64) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.digests))
	for key, digest := range s.digests {
		if !digest.DueAt.After(now) {
			keys = append(keys, key)
			if limit > 0 && int64(len(keys)) >= limit {
				break
			}
		}
	}
	return keys, nil
}

func (s *MemoryStore) Take(_ context.Context, key string) (Digest, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	digest, ok := s.digests[key]
	if !ok {
		return Digest{}, false, nil
	}
	delete(s.digests, key)
	return digest, true, nil
}

func (s *MemoryStore) PendingCount(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.digests)), nil
}

func (s *RedisStore) Append(ctx context.Context, key string, job rules.NotificationJob, dueAt time.Time, maxItems int) (Digest, error) {
	payloadKey := s.payloadKey(key)
	raw, err := s.client.Get(ctx, payloadKey).Result()
	if err != nil && err != redis.Nil {
		return Digest{}, err
	}

	now := time.Now().UTC()
	var digest Digest
	if err == redis.Nil {
		digest = newDigest(key, job, dueAt, now)
	} else if err := json.Unmarshal([]byte(raw), &digest); err != nil {
		return Digest{}, err
	}

	digest = appendItem(digest, job, maxItems, now)
	payload, err := json.Marshal(digest)
	if err != nil {
		return Digest{}, err
	}

	pipe := s.client.TxPipeline()
	pipe.Set(ctx, payloadKey, payload, time.Until(digest.DueAt)+time.Hour)
	pipe.ZAdd(ctx, s.dueKey, redis.Z{
		Score:  float64(digest.DueAt.Unix()),
		Member: key,
	})
	_, err = pipe.Exec(ctx)
	return digest, err
}

func (s *RedisStore) DueKeys(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	return s.client.ZRangeByScore(ctx, s.dueKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now.Unix(), 10),
		Offset: 0,
		Count:  limit,
	}).Result()
}

func (s *RedisStore) Take(ctx context.Context, key string) (Digest, bool, error) {
	payloadKey := s.payloadKey(key)
	raw, err := s.client.Get(ctx, payloadKey).Result()
	if err == redis.Nil {
		_, _ = s.client.ZRem(ctx, s.dueKey, key).Result()
		return Digest{}, false, nil
	}
	if err != nil {
		return Digest{}, false, err
	}

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, payloadKey)
	pipe.ZRem(ctx, s.dueKey, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return Digest{}, false, err
	}

	var digest Digest
	if err := json.Unmarshal([]byte(raw), &digest); err != nil {
		return Digest{}, false, err
	}
	return digest, true, nil
}

func (s *RedisStore) PendingCount(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.dueKey).Result()
}

func (s *RedisStore) payloadKey(key string) string {
	return s.payloadPrefix + ":" + strings.TrimSpace(key)
}

func newDigest(key string, job rules.NotificationJob, dueAt, now time.Time) Digest {
	return Digest{
		Key:            strings.TrimSpace(key),
		UserID:         strings.TrimSpace(job.Alert.UserID),
		ChatID:         strings.TrimSpace(job.ChatID),
		Channel:        strings.TrimSpace(job.Channel),
		SourceImportID: strings.TrimSpace(job.Alert.SourceImportID),
		CreatedAt:      now,
		DueAt:          dueAt.UTC(),
		LastUpdateAt:   now,
		Items:          make([]Item, 0, 8),
		Counts:         make(map[string]int),
	}
}

func appendItem(digest Digest, job rules.NotificationJob, maxItems int, now time.Time) Digest {
	item := Item{
		AlertID:        strings.TrimSpace(job.Alert.ID),
		Type:           strings.TrimSpace(job.Alert.Type),
		Severity:       strings.TrimSpace(job.Alert.Severity),
		Message:        strings.TrimSpace(job.Alert.Message),
		Category:       strings.TrimSpace(job.Alert.Category),
		Merchant:       strings.TrimSpace(job.Alert.Merchant),
		AmountCents:    job.Alert.AmountCents,
		SourceImportID: strings.TrimSpace(job.Alert.SourceImportID),
		TransactionID:  strings.TrimSpace(job.Alert.TransactionID),
		NotificationAt: now,
	}
	if maxItems <= 0 {
		maxItems = 20
	}
	if len(digest.Items) < maxItems {
		digest.Items = append(digest.Items, item)
	}
	digest.LastUpdateAt = now
	digest.Counts[item.Type]++
	return digest
}

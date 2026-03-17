package rules

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type MemoryStore struct {
	mu             sync.Mutex
	categoryTotals map[string]int64
	thresholds     map[string]struct{}
	merchants      map[string]struct{}
	alerts         map[string]struct{}
	transactions   map[string]struct{}
}

type RedisStore struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		categoryTotals: make(map[string]int64),
		thresholds:     make(map[string]struct{}),
		merchants:      make(map[string]struct{}),
		alerts:         make(map[string]struct{}),
		transactions:   make(map[string]struct{}),
	}
}

func NewRedisStore(client *redis.Client, prefix string, ttl time.Duration) *RedisStore {
	if strings.TrimSpace(prefix) == "" {
		prefix = "rules"
	}
	if ttl <= 0 {
		ttl = 45 * 24 * time.Hour
	}
	return &RedisStore{client: client, prefix: prefix, ttl: ttl}
}

func (s *MemoryStore) AddCategorySpend(_ context.Context, userID, month, category string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := categorySpendKey(userID, month, category)
	s.categoryTotals[key] += delta
	return s.categoryTotals[key], nil
}

func (s *MemoryStore) MarkThresholdTriggered(_ context.Context, userID, month, category, level string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := thresholdKey(userID, month, category, level)
	if _, exists := s.thresholds[key]; exists {
		return false, nil
	}
	s.thresholds[key] = struct{}{}
	return true, nil
}

func (s *MemoryStore) MarkMerchantSeen(_ context.Context, userID, merchant string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := merchantKey(userID, merchant)
	if _, exists := s.merchants[key]; exists {
		return false, nil
	}
	s.merchants[key] = struct{}{}
	return true, nil
}

func (s *MemoryStore) MarkAlertIssued(_ context.Context, userID, alertID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := alertKey(userID, alertID)
	if _, exists := s.alerts[key]; exists {
		return false, nil
	}
	s.alerts[key] = struct{}{}
	return true, nil
}

func (s *MemoryStore) MarkTransactionProcessed(_ context.Context, userID, transactionID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := transactionKey(userID, transactionID)
	if _, exists := s.transactions[key]; exists {
		return false, nil
	}
	s.transactions[key] = struct{}{}
	return true, nil
}

func (s *RedisStore) AddCategorySpend(ctx context.Context, userID, month, category string, delta int64) (int64, error) {
	key := s.prefixed(categorySpendKey(userID, month, category))
	total, err := s.client.IncrBy(ctx, key, delta).Result()
	if err != nil {
		return 0, err
	}
	_ = s.client.Expire(ctx, key, s.ttl).Err()
	return total, nil
}

func (s *RedisStore) MarkThresholdTriggered(ctx context.Context, userID, month, category, level string) (bool, error) {
	key := s.prefixed(thresholdKey(userID, month, category, level))
	created, err := s.client.SetNX(ctx, key, "1", s.ttl).Result()
	if err != nil {
		return false, err
	}
	return created, nil
}

func (s *RedisStore) MarkMerchantSeen(ctx context.Context, userID, merchant string) (bool, error) {
	key := s.prefixed("merchant:" + strings.TrimSpace(userID))
	added, err := s.client.SAdd(ctx, key, normalizeMerchant(merchant)).Result()
	if err != nil {
		return false, err
	}
	_ = s.client.Expire(ctx, key, 365*24*time.Hour).Err()
	return added == 1, nil
}

func (s *RedisStore) MarkAlertIssued(ctx context.Context, userID, alertID string) (bool, error) {
	key := s.prefixed(alertKey(userID, alertID))
	created, err := s.client.SetNX(ctx, key, "1", s.ttl).Result()
	if err != nil {
		return false, err
	}
	return created, nil
}

func (s *RedisStore) MarkTransactionProcessed(ctx context.Context, userID, transactionID string) (bool, error) {
	key := s.prefixed(transactionKey(userID, transactionID))
	created, err := s.client.SetNX(ctx, key, "1", s.ttl).Result()
	if err != nil {
		return false, err
	}
	return created, nil
}

func (s *RedisStore) prefixed(key string) string {
	return s.prefix + ":" + key
}

func categorySpendKey(userID, month, category string) string {
	return strings.Join([]string{"spend", strings.TrimSpace(userID), strings.TrimSpace(month), normalizeCategory(category)}, ":")
}

func thresholdKey(userID, month, category, level string) string {
	return strings.Join([]string{"threshold", strings.TrimSpace(userID), strings.TrimSpace(month), normalizeCategory(category), strings.TrimSpace(level)}, ":")
}

func merchantKey(userID, merchant string) string {
	return strings.Join([]string{"merchant", strings.TrimSpace(userID), normalizeMerchant(merchant)}, ":")
}

func alertKey(userID, alertID string) string {
	return strings.Join([]string{"alert", strings.TrimSpace(userID), strings.TrimSpace(alertID)}, ":")
}

func transactionKey(userID, transactionID string) string {
	return strings.Join([]string{"transaction", strings.TrimSpace(userID), strings.TrimSpace(transactionID)}, ":")
}

package telegramauth

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Binding struct {
	ChatID   string    `json:"chat_id"`
	UserID   string    `json:"user_id"`
	Username string    `json:"username"`
	Roles    []string  `json:"roles"`
	BoundAt  time.Time `json:"bound_at"`
}

type PendingLink struct {
	Code      string    `json:"code"`
	ChatID    string    `json:"chat_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Store interface {
	Save(ctx context.Context, binding Binding) error
	Get(ctx context.Context, chatID string) (Binding, bool, error)
	Delete(ctx context.Context, chatID string) error
}

type LinkStore interface {
	SavePending(ctx context.Context, pending PendingLink) error
	ConsumePending(ctx context.Context, code string) (PendingLink, bool, error)
}

type MemoryStore struct {
	mu       sync.RWMutex
	bindings map[string]Binding
	pending  map[string]PendingLink
}

type RedisStore struct {
	client        *redis.Client
	bindingPrefix string
	pendingPrefix string
	ttl           time.Duration
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		bindings: make(map[string]Binding),
		pending:  make(map[string]PendingLink),
	}
}

func NewRedisStore(client *redis.Client, prefix string, ttl time.Duration) *RedisStore {
	if strings.TrimSpace(prefix) == "" {
		prefix = "telegram:bindings"
	}
	if ttl <= 0 {
		ttl = 365 * 24 * time.Hour
	}
	return &RedisStore{
		client:        client,
		bindingPrefix: prefix,
		pendingPrefix: prefix + ":pending",
		ttl:           ttl,
	}
}

func (s *MemoryStore) Save(_ context.Context, binding Binding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindings[strings.TrimSpace(binding.ChatID)] = binding
	return nil
}

func (s *MemoryStore) Get(_ context.Context, chatID string) (Binding, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	binding, ok := s.bindings[strings.TrimSpace(chatID)]
	return binding, ok, nil
}

func (s *MemoryStore) Delete(_ context.Context, chatID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.bindings, strings.TrimSpace(chatID))
	return nil
}

func (s *MemoryStore) SavePending(_ context.Context, pending PendingLink) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[strings.TrimSpace(strings.ToUpper(pending.Code))] = pending
	return nil
}

func (s *MemoryStore) ConsumePending(_ context.Context, code string) (PendingLink, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := strings.TrimSpace(strings.ToUpper(code))
	pending, ok := s.pending[key]
	if !ok {
		return PendingLink{}, false, nil
	}
	delete(s.pending, key)
	if !pending.ExpiresAt.IsZero() && time.Now().UTC().After(pending.ExpiresAt) {
		return PendingLink{}, false, nil
	}
	return pending, true, nil
}

func (s *RedisStore) Save(ctx context.Context, binding Binding) error {
	payload, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.bindingKey(binding.ChatID), payload, s.ttl).Err()
}

func (s *RedisStore) Get(ctx context.Context, chatID string) (Binding, bool, error) {
	raw, err := s.client.Get(ctx, s.bindingKey(chatID)).Result()
	if err == redis.Nil {
		return Binding{}, false, nil
	}
	if err != nil {
		return Binding{}, false, err
	}
	var binding Binding
	if err := json.Unmarshal([]byte(raw), &binding); err != nil {
		return Binding{}, false, err
	}
	return binding, true, nil
}

func (s *RedisStore) Delete(ctx context.Context, chatID string) error {
	return s.client.Del(ctx, s.bindingKey(chatID)).Err()
}

func (s *RedisStore) SavePending(ctx context.Context, pending PendingLink) error {
	payload, err := json.Marshal(pending)
	if err != nil {
		return err
	}
	ttl := time.Until(pending.ExpiresAt)
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	return s.client.Set(ctx, s.pendingKey(pending.Code), payload, ttl).Err()
}

func (s *RedisStore) ConsumePending(ctx context.Context, code string) (PendingLink, bool, error) {
	raw, err := s.client.GetDel(ctx, s.pendingKey(code)).Result()
	if err == redis.Nil {
		return PendingLink{}, false, nil
	}
	if err != nil {
		return PendingLink{}, false, err
	}
	var pending PendingLink
	if err := json.Unmarshal([]byte(raw), &pending); err != nil {
		return PendingLink{}, false, err
	}
	if !pending.ExpiresAt.IsZero() && time.Now().UTC().After(pending.ExpiresAt) {
		return PendingLink{}, false, nil
	}
	return pending, true, nil
}

func (s *RedisStore) bindingKey(chatID string) string {
	return s.bindingPrefix + ":" + strings.TrimSpace(chatID)
}

func (s *RedisStore) pendingKey(code string) string {
	return s.pendingPrefix + ":" + strings.TrimSpace(strings.ToUpper(code))
}

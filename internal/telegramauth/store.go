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

type Store interface {
	Save(ctx context.Context, binding Binding) error
	Get(ctx context.Context, chatID string) (Binding, bool, error)
	Delete(ctx context.Context, chatID string) error
}

type MemoryStore struct {
	mu       sync.RWMutex
	bindings map[string]Binding
}

type RedisStore struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{bindings: make(map[string]Binding)}
}

func NewRedisStore(client *redis.Client, prefix string, ttl time.Duration) *RedisStore {
	if strings.TrimSpace(prefix) == "" {
		prefix = "telegram:bindings"
	}
	if ttl <= 0 {
		ttl = 365 * 24 * time.Hour
	}
	return &RedisStore{client: client, prefix: prefix, ttl: ttl}
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

func (s *RedisStore) Save(ctx context.Context, binding Binding) error {
	payload, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.key(binding.ChatID), payload, s.ttl).Err()
}

func (s *RedisStore) Get(ctx context.Context, chatID string) (Binding, bool, error) {
	raw, err := s.client.Get(ctx, s.key(chatID)).Result()
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
	return s.client.Del(ctx, s.key(chatID)).Err()
}

func (s *RedisStore) key(chatID string) string {
	return s.prefix + ":" + strings.TrimSpace(chatID)
}

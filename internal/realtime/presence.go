package realtime

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"personal-finance-os/internal/platform/ws"
)

type PresenceStore interface {
	UpsertConnection(context.Context, ws.ClientInfo) error
	RemoveConnection(context.Context, ws.ClientInfo) error
	RefreshConnections(context.Context, []ws.ClientInfo) error
	PresenceCount(context.Context, string) (int64, error)
}

type RedisStore struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func NewRedisStore(client *redis.Client, prefix string, ttl time.Duration) *RedisStore {
	if strings.TrimSpace(prefix) == "" {
		prefix = "ws"
	}
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	return &RedisStore{
		client: client,
		prefix: strings.TrimSpace(prefix),
		ttl:    ttl,
	}
}

func (s *RedisStore) UpsertConnection(ctx context.Context, info ws.ClientInfo) error {
	subscriptions, err := json.Marshal(info.Channels)
	if err != nil {
		return err
	}

	pipeline := s.client.TxPipeline()
	pipeline.SAdd(ctx, s.presenceKey(info.UserID), info.ConnectionID)
	pipeline.Expire(ctx, s.presenceKey(info.UserID), s.ttl)
	pipeline.Set(ctx, s.subscriptionsKey(info.ConnectionID), subscriptions, s.ttl)
	_, err = pipeline.Exec(ctx)
	return err
}

func (s *RedisStore) RemoveConnection(ctx context.Context, info ws.ClientInfo) error {
	pipeline := s.client.TxPipeline()
	pipeline.SRem(ctx, s.presenceKey(info.UserID), info.ConnectionID)
	pipeline.Del(ctx, s.subscriptionsKey(info.ConnectionID))
	_, err := pipeline.Exec(ctx)
	return err
}

func (s *RedisStore) RefreshConnections(ctx context.Context, infos []ws.ClientInfo) error {
	if len(infos) == 0 {
		return nil
	}

	pipeline := s.client.TxPipeline()
	for _, info := range infos {
		subscriptions, err := json.Marshal(info.Channels)
		if err != nil {
			return err
		}
		pipeline.SAdd(ctx, s.presenceKey(info.UserID), info.ConnectionID)
		pipeline.Expire(ctx, s.presenceKey(info.UserID), s.ttl)
		pipeline.Set(ctx, s.subscriptionsKey(info.ConnectionID), subscriptions, s.ttl)
	}
	_, err := pipeline.Exec(ctx)
	return err
}

func (s *RedisStore) PresenceCount(ctx context.Context, userID string) (int64, error) {
	return s.client.SCard(ctx, s.presenceKey(userID)).Result()
}

func (s *RedisStore) presenceKey(userID string) string {
	return s.prefix + ":presence:" + normalizePart(userID)
}

func (s *RedisStore) subscriptionsKey(connectionID string) string {
	return s.prefix + ":subscriptions:" + normalizePart(connectionID)
}

func normalizePart(value string) string {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return "unknown"
	}
	return normalized
}

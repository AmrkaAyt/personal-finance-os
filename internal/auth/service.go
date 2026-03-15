package auth

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"personal-finance-os/internal/platform/jwtx"
)

var ErrInvalidCredentials = errors.New("invalid credentials")

type User struct {
	ID       string
	Username string
	Password string
	Roles    []string
}

type SessionStore interface {
	Save(ctx context.Context, tokenID, userID string, expiresAt time.Time) error
	Get(ctx context.Context, tokenID string) (string, error)
	Delete(ctx context.Context, tokenID string) error
}

type Service struct {
	tokens   *jwtx.Manager
	sessions SessionStore
	users    map[string]User
}

type MemorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]memorySession
}

type memorySession struct {
	UserID    string
	ExpiresAt time.Time
}

type RedisSessionStore struct {
	client *redis.Client
	prefix string
}

func DefaultUsers() map[string]User {
	return map[string]User{
		"demo": {
			ID:       "user-demo",
			Username: "demo",
			Password: "demo",
			Roles:    []string{"owner", "member"},
		},
		"advisor": {
			ID:       "user-advisor",
			Username: "advisor",
			Password: "advisor",
			Roles:    []string{"advisor_readonly"},
		},
	}
}

func NewService(tokens *jwtx.Manager, sessions SessionStore, users map[string]User) *Service {
	return &Service{tokens: tokens, sessions: sessions, users: users}
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{sessions: make(map[string]memorySession)}
}

func NewRedisSessionStore(client *redis.Client, prefix string) *RedisSessionStore {
	return &RedisSessionStore{client: client, prefix: prefix}
}

func (s *Service) Login(ctx context.Context, username, password string) (jwtx.TokenPair, error) {
	user, ok := s.users[username]
	if !ok || user.Password != password {
		return jwtx.TokenPair{}, ErrInvalidCredentials
	}
	pair, refreshTokenID, err := s.tokens.IssuePair(user.ID, user.Roles)
	if err != nil {
		return jwtx.TokenPair{}, err
	}
	claims, err := s.tokens.Parse(pair.RefreshToken)
	if err != nil {
		return jwtx.TokenPair{}, err
	}
	if err := s.sessions.Save(ctx, refreshTokenID, user.ID, claims.ExpiresAt.Time); err != nil {
		return jwtx.TokenPair{}, err
	}
	return pair, nil
}

func (s *Service) Refresh(ctx context.Context, refreshToken string) (jwtx.TokenPair, error) {
	claims, err := s.tokens.Parse(refreshToken)
	if err != nil {
		return jwtx.TokenPair{}, err
	}
	if claims.Type != "refresh" {
		return jwtx.TokenPair{}, errors.New("token is not a refresh token")
	}
	userID, err := s.sessions.Get(ctx, claims.ID)
	if err != nil {
		return jwtx.TokenPair{}, err
	}
	if userID != claims.Subject {
		return jwtx.TokenPair{}, errors.New("refresh token subject mismatch")
	}
	_ = s.sessions.Delete(ctx, claims.ID)

	user, ok := s.findUserByID(userID)
	if !ok {
		return jwtx.TokenPair{}, errors.New("user not found")
	}
	pair, refreshTokenID, err := s.tokens.IssuePair(user.ID, user.Roles)
	if err != nil {
		return jwtx.TokenPair{}, err
	}
	newClaims, err := s.tokens.Parse(pair.RefreshToken)
	if err != nil {
		return jwtx.TokenPair{}, err
	}
	if err := s.sessions.Save(ctx, refreshTokenID, user.ID, newClaims.ExpiresAt.Time); err != nil {
		return jwtx.TokenPair{}, err
	}
	return pair, nil
}

func (s *Service) findUserByID(userID string) (User, bool) {
	for _, user := range s.users {
		if user.ID == userID {
			return user, true
		}
	}
	return User{}, false
}

func (s *MemorySessionStore) Save(_ context.Context, tokenID, userID string, expiresAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[tokenID] = memorySession{UserID: userID, ExpiresAt: expiresAt}
	return nil
}

func (s *MemorySessionStore) Get(_ context.Context, tokenID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[tokenID]
	if !ok || time.Now().After(session.ExpiresAt) {
		return "", redis.Nil
	}
	return session.UserID, nil
}

func (s *MemorySessionStore) Delete(_ context.Context, tokenID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, tokenID)
	return nil
}

func (s *RedisSessionStore) Save(ctx context.Context, tokenID, userID string, expiresAt time.Time) error {
	return s.client.Set(ctx, s.key(tokenID), userID, time.Until(expiresAt)).Err()
}

func (s *RedisSessionStore) Get(ctx context.Context, tokenID string) (string, error) {
	return s.client.Get(ctx, s.key(tokenID)).Result()
}

func (s *RedisSessionStore) Delete(ctx context.Context, tokenID string) error {
	return s.client.Del(ctx, s.key(tokenID)).Err()
}

func (s *RedisSessionStore) key(tokenID string) string {
	return s.prefix + ":" + tokenID
}

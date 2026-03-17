package ws

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultUserID  = "anonymous"
	defaultChannel = "dashboard"
)

type ClientInfo struct {
	ConnectionID string    `json:"connection_id"`
	UserID       string    `json:"user_id"`
	Channels     []string  `json:"channels"`
	ConnectedAt  time.Time `json:"connected_at"`
}

type Hooks struct {
	OnConnect              func(ClientInfo)
	OnDisconnect           func(ClientInfo)
	OnSubscriptionsChanged func(ClientInfo)
}

type Option func(*Hub)

type Hub struct {
	logger   *slog.Logger
	upgrader websocket.Upgrader
	hooks    Hooks

	mu      sync.RWMutex
	clients map[string]*client
}

type client struct {
	conn      *websocket.Conn
	info      ClientInfo
	channels  map[string]struct{}
	writeMu   sync.Mutex
	closeOnce sync.Once
}

type inboundMessage struct {
	Type     string   `json:"type"`
	Channels []string `json:"channels"`
}

func NewHub(logger *slog.Logger, options ...Option) *Hub {
	hub := &Hub{
		logger: logger,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
		clients: make(map[string]*client),
	}
	for _, option := range options {
		option(hub)
	}
	return hub
}

func WithHooks(hooks Hooks) Option {
	return func(hub *Hub) {
		hub.hooks = hooks
	}
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	client := newClient(conn, r)
	h.registerClient(client)

	if err := client.writeJSON(map[string]any{
		"type":          "system.connected",
		"connection_id": client.info.ConnectionID,
		"user_id":       client.info.UserID,
		"channels":      client.snapshot().Channels,
		"connected_at":  client.info.ConnectedAt,
	}); err != nil {
		h.disconnectClient(client)
		return
	}

	go h.readLoop(client)
}

func (h *Hub) BroadcastJSON(payload any) {
	h.broadcast(payload, func(*client) bool { return true })
}

func (h *Hub) BroadcastToChannels(channels []string, payload any) {
	required := normalizeChannels(channels)
	h.broadcast(payload, func(client *client) bool {
		return client.matches("", required)
	})
}

func (h *Hub) BroadcastToUserChannels(userID string, channels []string, payload any) {
	required := normalizeChannels(channels)
	h.broadcast(payload, func(client *client) bool {
		return client.matches(strings.TrimSpace(userID), required)
	})
}

func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) CountByUser(userID string) int {
	target := strings.TrimSpace(userID)
	if target == "" {
		return h.Count()
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	count := 0
	for _, client := range h.clients {
		if client.info.UserID == target {
			count++
		}
	}
	return count
}

func (h *Hub) Snapshot() []ClientInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snapshot := make([]ClientInfo, 0, len(h.clients))
	for _, client := range h.clients {
		snapshot = append(snapshot, client.snapshot())
	}
	sort.Slice(snapshot, func(i, j int) bool {
		if snapshot[i].UserID == snapshot[j].UserID {
			return snapshot[i].ConnectionID < snapshot[j].ConnectionID
		}
		return snapshot[i].UserID < snapshot[j].UserID
	})
	return snapshot
}

func (h *Hub) registerClient(client *client) {
	h.mu.Lock()
	h.clients[client.info.ConnectionID] = client
	h.mu.Unlock()

	h.logger.Info("websocket connected", "connection_id", client.info.ConnectionID, "user_id", client.info.UserID, "clients", h.Count())
	h.runHook(h.hooks.OnConnect, client.snapshot())
}

func (h *Hub) disconnectClient(client *client) {
	client.closeOnce.Do(func() {
		h.mu.Lock()
		delete(h.clients, client.info.ConnectionID)
		h.mu.Unlock()

		_ = client.conn.Close()
		info := client.snapshot()
		h.logger.Info("websocket disconnected", "connection_id", info.ConnectionID, "user_id", info.UserID, "clients", h.Count())
		h.runHook(h.hooks.OnDisconnect, info)
	})
}

func (h *Hub) broadcast(payload any, filter func(*client) bool) {
	h.mu.RLock()
	clients := make([]*client, 0, len(h.clients))
	for _, client := range h.clients {
		if filter(client) {
			clients = append(clients, client)
		}
	}
	h.mu.RUnlock()

	for _, client := range clients {
		if err := client.writeJSON(payload); err != nil {
			h.logger.Warn("websocket broadcast failed", "connection_id", client.info.ConnectionID, "error", err)
			h.disconnectClient(client)
		}
	}
}

func (h *Hub) readLoop(client *client) {
	defer h.disconnectClient(client)

	client.conn.SetReadLimit(8 * 1024)
	_ = client.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	client.conn.SetPongHandler(func(string) error {
		return client.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	})

	for {
		messageType, message, err := client.conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		if err := h.handleInboundMessage(client, message); err != nil {
			if writeErr := client.writeJSON(map[string]any{
				"type":    "system.error",
				"message": err.Error(),
			}); writeErr != nil {
				return
			}
		}
	}
}

func (h *Hub) handleInboundMessage(client *client, payload []byte) error {
	var message inboundMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return err
	}

	switch strings.TrimSpace(strings.ToLower(message.Type)) {
	case "ping":
		return client.writeJSON(map[string]any{
			"type":      "pong",
			"timestamp": time.Now().UTC(),
		})
	case "subscribe":
		client.addChannels(message.Channels)
		info := client.snapshot()
		h.runHook(h.hooks.OnSubscriptionsChanged, info)
		return client.writeJSON(map[string]any{
			"type":     "subscriptions.updated",
			"channels": info.Channels,
		})
	case "unsubscribe":
		client.removeChannels(message.Channels)
		info := client.snapshot()
		h.runHook(h.hooks.OnSubscriptionsChanged, info)
		return client.writeJSON(map[string]any{
			"type":     "subscriptions.updated",
			"channels": info.Channels,
		})
	default:
		return client.writeJSON(map[string]any{
			"type":    "system.error",
			"message": "unsupported websocket message type",
		})
	}
}

func (h *Hub) runHook(hook func(ClientInfo), info ClientInfo) {
	if hook == nil {
		return
	}
	hook(info)
}

func newClient(conn *websocket.Conn, r *http.Request) *client {
	info := ClientInfo{
		ConnectionID: firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("connection_id")), generateConnectionID()),
		UserID:       firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("user_id")), defaultUserID),
		Channels:     normalizeChannels(strings.Split(strings.TrimSpace(r.URL.Query().Get("channels")), ",")),
		ConnectedAt:  time.Now().UTC(),
	}

	channelSet := make(map[string]struct{}, len(info.Channels))
	for _, channel := range info.Channels {
		channelSet[channel] = struct{}{}
	}

	return &client{
		conn:     conn,
		info:     info,
		channels: channelSet,
	}
}

func (c *client) writeJSON(payload any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(payload)
}

func (c *client) snapshot() ClientInfo {
	channels := make([]string, 0, len(c.channels))
	for channel := range c.channels {
		channels = append(channels, channel)
	}
	sort.Strings(channels)

	return ClientInfo{
		ConnectionID: c.info.ConnectionID,
		UserID:       c.info.UserID,
		Channels:     channels,
		ConnectedAt:  c.info.ConnectedAt,
	}
}

func (c *client) matches(userID string, channels []string) bool {
	if userID != "" && c.info.UserID != userID {
		return false
	}
	if len(channels) == 0 {
		return true
	}
	for _, channel := range channels {
		if _, ok := c.channels[channel]; ok {
			return true
		}
	}
	return false
}

func (c *client) addChannels(channels []string) {
	for _, channel := range normalizeChannels(channels) {
		c.channels[channel] = struct{}{}
	}
}

func (c *client) removeChannels(channels []string) {
	for _, channel := range normalizeChannels(channels) {
		delete(c.channels, channel)
	}
}

func normalizeChannels(channels []string) []string {
	normalized := make([]string, 0, len(channels))
	seen := make(map[string]struct{}, len(channels))
	for _, channel := range channels {
		value := strings.TrimSpace(strings.ToLower(channel))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	if len(normalized) == 0 {
		return []string{defaultChannel}
	}
	sort.Strings(normalized)
	return normalized
}

func generateConnectionID() string {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err == nil {
		return "ws-" + hex.EncodeToString(buffer)
	}
	return "ws-" + strconvTimestamp()
}

func strconvTimestamp() string {
	return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

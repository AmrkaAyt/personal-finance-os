package ws

import (
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	logger   *slog.Logger
	upgrader websocket.Upgrader
	mu       sync.RWMutex
	clients  map[*websocket.Conn]struct{}
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		logger: logger,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(_ *http.Request) bool { return true },
		},
		clients: make(map[*websocket.Conn]struct{}),
	}
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	h.mu.Lock()
	h.clients[conn] = struct{}{}
	h.mu.Unlock()
	h.logger.Info("websocket connected", "clients", h.Count())

	go h.readLoop(conn)
}

func (h *Hub) BroadcastJSON(payload any) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for client := range h.clients {
		_ = client.WriteJSON(payload)
	}
}

func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) readLoop(conn *websocket.Conn) {
	defer func() {
		h.mu.Lock()
		delete(h.clients, conn)
		h.mu.Unlock()
		_ = conn.Close()
		h.logger.Info("websocket disconnected", "clients", h.Count())
	}()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if err := conn.WriteMessage(messageType, message); err != nil {
			return
		}
	}
}

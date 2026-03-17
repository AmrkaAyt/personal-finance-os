package realtime

import (
	"strings"
	"time"

	"personal-finance-os/internal/ledger"
	"personal-finance-os/internal/rules"
)

const (
	ChannelDashboard    = "dashboard"
	ChannelTransactions = "transactions"
	ChannelAlerts       = "alerts"
)

type Envelope struct {
	Type      string    `json:"type"`
	Channel   string    `json:"channel"`
	UserID    string    `json:"user_id"`
	Payload   any       `json:"payload"`
	EmittedAt time.Time `json:"emitted_at"`
}

func TransactionEnvelope(event ledger.TransactionUpsertedEvent) Envelope {
	return Envelope{
		Type:      "transaction.upserted",
		Channel:   ChannelTransactions,
		UserID:    strings.TrimSpace(event.UserID),
		Payload:   event,
		EmittedAt: time.Now().UTC(),
	}
}

func AlertEnvelope(alert rules.Alert) Envelope {
	return Envelope{
		Type:      "alert.created",
		Channel:   ChannelAlerts,
		UserID:    strings.TrimSpace(alert.UserID),
		Payload:   alert,
		EmittedAt: time.Now().UTC(),
	}
}

func PresenceEnvelope(totalConnections int, activeUsers int) Envelope {
	return Envelope{
		Type:    "presence.snapshot",
		Channel: ChannelDashboard,
		Payload: map[string]any{
			"connections":  totalConnections,
			"active_users": activeUsers,
		},
		EmittedAt: time.Now().UTC(),
	}
}

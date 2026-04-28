package insights

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	ActionAcknowledge      = "acknowledge"
	ActionSnooze           = "snooze"
	ActionResolve          = "resolve"
	ActionSuppressSimilar  = "suppress_similar"
	ActionConfirmRecurring = "confirm_recurring"
	ActionRejectRecurring  = "reject_recurring"
	ActionRecategorize     = "recategorize"
)

var allowedActions = []string{
	ActionAcknowledge,
	ActionSnooze,
	ActionResolve,
	ActionSuppressSimilar,
	ActionConfirmRecurring,
	ActionRejectRecurring,
	ActionRecategorize,
}

type Action struct {
	ID           string            `json:"id"`
	UserID       string            `json:"user_id"`
	InsightID    string            `json:"insight_id"`
	InsightType  string            `json:"insight_type"`
	Action       string            `json:"action"`
	Reason       string            `json:"reason,omitempty"`
	SnoozedUntil *time.Time        `json:"snoozed_until,omitempty"`
	Metadata     map[string]string `json:"metadata"`
	CreatedAt    time.Time         `json:"created_at,omitempty"`
	UpdatedAt    time.Time         `json:"updated_at,omitempty"`
}

func Normalize(input Action) (Action, error) {
	result := input
	result.UserID = strings.TrimSpace(result.UserID)
	result.InsightID = strings.TrimSpace(result.InsightID)
	result.InsightType = normalizeToken(result.InsightType)
	result.Action = normalizeToken(result.Action)
	result.Reason = strings.TrimSpace(result.Reason)
	result.Metadata = normalizeMetadata(result.Metadata)
	if result.UserID == "" {
		return Action{}, fmt.Errorf("user_id is required")
	}
	if result.InsightID == "" {
		return Action{}, fmt.Errorf("insight_id is required")
	}
	if result.InsightType == "" {
		return Action{}, fmt.Errorf("insight_type is required")
	}
	if !slices.Contains(allowedActions, result.Action) {
		return Action{}, fmt.Errorf("unsupported action")
	}
	if result.Action == ActionSnooze && result.SnoozedUntil == nil {
		return Action{}, fmt.Errorf("snoozed_until is required for snooze")
	}
	if result.SnoozedUntil != nil {
		utc := result.SnoozedUntil.UTC()
		result.SnoozedUntil = &utc
	}
	result.ID = ActionID(result.UserID, result.InsightID)
	return result, nil
}

func ActionID(userID, insightID string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(userID),
		strings.TrimSpace(insightID),
	}, "|")))
	return "insact-" + hex.EncodeToString(sum[:10])
}

func normalizeToken(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeMetadata(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		normalizedKey := normalizeToken(key)
		if normalizedKey == "" {
			continue
		}
		result[normalizedKey] = strings.TrimSpace(value)
	}
	return result
}

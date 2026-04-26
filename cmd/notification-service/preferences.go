package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"personal-finance-os/internal/notificationprefs"
	"personal-finance-os/internal/platform/httpx"
	"personal-finance-os/internal/rules"
)

type deliveryMode string

const (
	deliveryImmediate deliveryMode = "immediate"
	deliveryBatch     deliveryMode = "batch"
	deliverySuppress  deliveryMode = "suppress"
)

type notificationPreferencesRequest struct {
	TelegramEnabled    bool     `json:"telegram_enabled"`
	BatchNonCritical   bool     `json:"batch_non_critical"`
	QuietHoursEnabled  bool     `json:"quiet_hours_enabled"`
	QuietStartMinute   int      `json:"quiet_start_minute"`
	QuietEndMinute     int      `json:"quiet_end_minute"`
	QuietTimezone      string   `json:"quiet_timezone"`
	DisabledAlertTypes []string `json:"disabled_alert_types"`
}

func (s *service) handlePreferencesGet(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if userID == "" {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	prefs, err := s.loadPreferences(r.Context(), userID)
	if err != nil {
		s.logger.Error("failed to load notification preferences", "user_id", userID, "error", err)
		httpx.JSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to load notification preferences"})
		return
	}
	httpx.JSON(w, http.StatusOK, prefs)
}

func (s *service) handlePreferencesPut(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if userID == "" {
		httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	var request notificationPreferencesRequest
	if err := httpx.ReadJSON(r, &request); err != nil {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}

	stored, err := s.prefsStore.Upsert(r.Context(), notificationprefs.Preferences{
		UserID:             userID,
		TelegramEnabled:    request.TelegramEnabled,
		BatchNonCritical:   request.BatchNonCritical,
		QuietHoursEnabled:  request.QuietHoursEnabled,
		QuietStartMinute:   request.QuietStartMinute,
		QuietEndMinute:     request.QuietEndMinute,
		QuietTimezone:      strings.TrimSpace(request.QuietTimezone),
		DisabledAlertTypes: request.DisabledAlertTypes,
	})
	if err != nil {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	httpx.JSON(w, http.StatusOK, stored)
}

func (s *service) loadPreferences(ctx context.Context, userID string) (notificationprefs.Preferences, error) {
	if s.prefsStore == nil || strings.TrimSpace(userID) == "" {
		return notificationprefs.Default(userID), nil
	}
	return s.prefsStore.Get(ctx, userID)
}

func (s *service) deliveryDecision(ctx context.Context, job rules.NotificationJob) (deliveryMode, time.Time, error) {
	if isSystemNotification(job) {
		return deliveryImmediate, time.Time{}, nil
	}

	prefs, err := s.loadPreferences(ctx, strings.TrimSpace(job.Alert.UserID))
	if err != nil {
		return "", time.Time{}, err
	}
	if !prefs.TelegramEnabled {
		return deliverySuppress, time.Time{}, nil
	}
	if prefs.AlertTypeDisabled(job.Alert.Type) {
		return deliverySuppress, time.Time{}, nil
	}
	if !strings.EqualFold(strings.TrimSpace(job.Alert.Severity), "critical") {
		if active, until, err := prefs.QuietWindow(time.Now().UTC()); err != nil {
			return "", time.Time{}, err
		} else if active {
			return deliveryBatch, until, nil
		}
	}
	if prefs.BatchNonCritical && s.shouldBatch(job) {
		return deliveryBatch, time.Now().UTC().Add(s.digestWindow), nil
	}
	return deliveryImmediate, time.Time{}, nil
}

func isSystemNotification(job rules.NotificationJob) bool {
	switch strings.TrimSpace(job.Alert.Type) {
	case "", "telegram_command", "telegram_link_confirmed", "digest", "demo":
		return true
	default:
		return false
	}
}

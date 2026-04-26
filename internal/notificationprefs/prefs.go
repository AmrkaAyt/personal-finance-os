package notificationprefs

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

type Preferences struct {
	UserID             string    `json:"user_id"`
	TelegramEnabled    bool      `json:"telegram_enabled"`
	BatchNonCritical   bool      `json:"batch_non_critical"`
	QuietHoursEnabled  bool      `json:"quiet_hours_enabled"`
	QuietStartMinute   int       `json:"quiet_start_minute"`
	QuietEndMinute     int       `json:"quiet_end_minute"`
	QuietTimezone      string    `json:"quiet_timezone"`
	DisabledAlertTypes []string  `json:"disabled_alert_types"`
	CreatedAt          time.Time `json:"created_at,omitempty"`
	UpdatedAt          time.Time `json:"updated_at,omitempty"`
}

func Default(userID string) Preferences {
	return Preferences{
		UserID:             strings.TrimSpace(userID),
		TelegramEnabled:    true,
		BatchNonCritical:   true,
		QuietHoursEnabled:  false,
		QuietStartMinute:   23 * 60,
		QuietEndMinute:     8 * 60,
		QuietTimezone:      "UTC",
		DisabledAlertTypes: []string{},
	}
}

func Normalize(input Preferences) (Preferences, error) {
	result := input
	result.UserID = strings.TrimSpace(result.UserID)
	if result.UserID == "" {
		return Preferences{}, fmt.Errorf("user_id is required")
	}
	if strings.TrimSpace(result.QuietTimezone) == "" {
		result.QuietTimezone = "UTC"
	}
	if _, err := time.LoadLocation(result.QuietTimezone); err != nil {
		return Preferences{}, fmt.Errorf("invalid quiet_timezone: %w", err)
	}
	if result.QuietStartMinute < 0 || result.QuietStartMinute > 1439 {
		return Preferences{}, fmt.Errorf("quiet_start_minute must be between 0 and 1439")
	}
	if result.QuietEndMinute < 0 || result.QuietEndMinute > 1439 {
		return Preferences{}, fmt.Errorf("quiet_end_minute must be between 0 and 1439")
	}

	cleanTypes := make([]string, 0, len(result.DisabledAlertTypes))
	seen := make(map[string]struct{}, len(result.DisabledAlertTypes))
	for _, item := range result.DisabledAlertTypes {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		cleanTypes = append(cleanTypes, value)
	}
	slices.Sort(cleanTypes)
	result.DisabledAlertTypes = cleanTypes
	return result, nil
}

func (p Preferences) AlertTypeDisabled(alertType string) bool {
	return slices.Contains(p.DisabledAlertTypes, strings.TrimSpace(alertType))
}

func (p Preferences) QuietWindow(now time.Time) (bool, time.Time, error) {
	if !p.QuietHoursEnabled {
		return false, time.Time{}, nil
	}
	location, err := time.LoadLocation(strings.TrimSpace(p.QuietTimezone))
	if err != nil {
		return false, time.Time{}, err
	}
	local := now.In(location)
	minuteOfDay := local.Hour()*60 + local.Minute()
	start := p.QuietStartMinute
	end := p.QuietEndMinute
	if start == end {
		return true, nextLocalMinute(local, end).UTC(), nil
	}

	active := false
	if start < end {
		active = minuteOfDay >= start && minuteOfDay < end
	} else {
		active = minuteOfDay >= start || minuteOfDay < end
	}
	if !active {
		return false, time.Time{}, nil
	}

	endTime := nextLocalMinute(local, end)
	if start < end {
		endTime = time.Date(local.Year(), local.Month(), local.Day(), end/60, end%60, 0, 0, location)
	}
	return true, endTime.UTC(), nil
}

func nextLocalMinute(now time.Time, targetMinute int) time.Time {
	candidate := time.Date(now.Year(), now.Month(), now.Day(), targetMinute/60, targetMinute%60, 0, 0, now.Location())
	if !candidate.After(now) {
		candidate = candidate.Add(24 * time.Hour)
	}
	return candidate
}

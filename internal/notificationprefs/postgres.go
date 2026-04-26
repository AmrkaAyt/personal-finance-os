package notificationprefs

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	Get(context.Context, string) (Preferences, error)
	Upsert(context.Context, Preferences) (Preferences, error)
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Get(ctx context.Context, userID string) (Preferences, error) {
	var prefs Preferences
	row := s.pool.QueryRow(ctx, `
		select
			user_id,
			telegram_enabled,
			batch_non_critical,
			quiet_hours_enabled,
			quiet_start_minute,
			quiet_end_minute,
			quiet_timezone,
			disabled_alert_types,
			created_at,
			updated_at
		from notification_preferences
		where user_id = $1
	`, userID)
	err := row.Scan(
		&prefs.UserID,
		&prefs.TelegramEnabled,
		&prefs.BatchNonCritical,
		&prefs.QuietHoursEnabled,
		&prefs.QuietStartMinute,
		&prefs.QuietEndMinute,
		&prefs.QuietTimezone,
		&prefs.DisabledAlertTypes,
		&prefs.CreatedAt,
		&prefs.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Default(userID), nil
	}
	return prefs, err
}

func (s *PostgresStore) Upsert(ctx context.Context, prefs Preferences) (Preferences, error) {
	normalized, err := Normalize(prefs)
	if err != nil {
		return Preferences{}, err
	}
	row := s.pool.QueryRow(ctx, `
		insert into notification_preferences (
			user_id,
			telegram_enabled,
			batch_non_critical,
			quiet_hours_enabled,
			quiet_start_minute,
			quiet_end_minute,
			quiet_timezone,
			disabled_alert_types
		) values ($1,$2,$3,$4,$5,$6,$7,$8)
		on conflict (user_id) do update set
			telegram_enabled = excluded.telegram_enabled,
			batch_non_critical = excluded.batch_non_critical,
			quiet_hours_enabled = excluded.quiet_hours_enabled,
			quiet_start_minute = excluded.quiet_start_minute,
			quiet_end_minute = excluded.quiet_end_minute,
			quiet_timezone = excluded.quiet_timezone,
			disabled_alert_types = excluded.disabled_alert_types,
			updated_at = now()
		returning
			user_id,
			telegram_enabled,
			batch_non_critical,
			quiet_hours_enabled,
			quiet_start_minute,
			quiet_end_minute,
			quiet_timezone,
			disabled_alert_types,
			created_at,
			updated_at
	`,
		normalized.UserID,
		normalized.TelegramEnabled,
		normalized.BatchNonCritical,
		normalized.QuietHoursEnabled,
		normalized.QuietStartMinute,
		normalized.QuietEndMinute,
		normalized.QuietTimezone,
		normalized.DisabledAlertTypes,
	)
	var stored Preferences
	err = row.Scan(
		&stored.UserID,
		&stored.TelegramEnabled,
		&stored.BatchNonCritical,
		&stored.QuietHoursEnabled,
		&stored.QuietStartMinute,
		&stored.QuietEndMinute,
		&stored.QuietTimezone,
		&stored.DisabledAlertTypes,
		&stored.CreatedAt,
		&stored.UpdatedAt,
	)
	return stored, err
}

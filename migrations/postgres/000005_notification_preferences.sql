create table if not exists notification_preferences (
    user_id text primary key,
    telegram_enabled boolean not null default true,
    batch_non_critical boolean not null default true,
    quiet_hours_enabled boolean not null default false,
    quiet_start_minute integer not null default 1380,
    quiet_end_minute integer not null default 480,
    quiet_timezone text not null default 'UTC',
    disabled_alert_types text[] not null default '{}'::text[],
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint notification_preferences_quiet_start_range check (quiet_start_minute between 0 and 1439),
    constraint notification_preferences_quiet_end_range check (quiet_end_minute between 0 and 1439)
);

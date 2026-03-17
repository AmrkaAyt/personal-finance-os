CREATE TABLE IF NOT EXISTS {{DATABASE}}.transaction_events (
    event_id String,
    event_date Date,
    user_id String,
    category LowCardinality(String),
    merchant String,
    amount_cents Int64,
    debit_cents UInt64,
    credit_cents UInt64,
    currency LowCardinality(String),
    transaction_id String,
    source_import_id String,
    occurred_at DateTime64(3, 'UTC'),
    recorded_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(recorded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, event_date, event_id);

CREATE TABLE IF NOT EXISTS {{DATABASE}}.alert_events (
    event_id String,
    event_date Date,
    user_id String,
    type LowCardinality(String),
    severity LowCardinality(String),
    category LowCardinality(String),
    merchant String,
    message String,
    amount_cents Int64,
    transaction_id String,
    source_import_id String,
    created_at DateTime64(3, 'UTC'),
    recorded_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(recorded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, event_date, event_id);

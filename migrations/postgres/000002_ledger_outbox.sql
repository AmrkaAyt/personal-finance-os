CREATE TABLE IF NOT EXISTS outbox_events (
    id text PRIMARY KEY,
    topic text NOT NULL,
    message_key text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    publish_attempts integer NOT NULL DEFAULT 0,
    last_error text NOT NULL DEFAULT '',
    claim_owner text,
    claimed_at timestamptz,
    claim_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending_created_at
ON outbox_events (created_at ASC)
WHERE published_at IS NULL;

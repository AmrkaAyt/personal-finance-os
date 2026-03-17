CREATE TABLE IF NOT EXISTS categories (
    name text PRIMARY KEY,
    kind text NOT NULL DEFAULT 'system',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    id text PRIMARY KEY,
    user_id text NOT NULL,
    account_id text NOT NULL,
    source_import_id text NOT NULL,
    fingerprint text NOT NULL,
    merchant text NOT NULL,
    category text NOT NULL,
    amount_cents bigint NOT NULL,
    currency text NOT NULL,
    occurred_at timestamptz NOT NULL,
    raw_line text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_occurred_at ON transactions (user_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_account_occurred_at ON transactions (account_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_source_import_id ON transactions (source_import_id);

CREATE INDEX IF NOT EXISTS idx_transactions_merchant_occurred_at ON transactions (merchant, occurred_at DESC);

ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_source_fingerprint_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_user_source_fingerprint ON transactions (user_id, source_import_id, fingerprint);

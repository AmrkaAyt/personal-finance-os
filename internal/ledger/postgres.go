package ledger

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) EnsureSchema(ctx context.Context) error {
	schema := `
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
`
	if _, err := r.pool.Exec(ctx, schema); err != nil {
		return err
	}

	batch := &pgx.Batch{}
	for _, category := range DefaultCategories {
		batch.Queue(`
INSERT INTO categories (name, kind)
VALUES ($1, 'system')
ON CONFLICT (name) DO NOTHING
`, category)
	}
	results := r.pool.SendBatch(ctx, batch)
	for range DefaultCategories {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	return results.Close()
}

func (r *Repository) UpsertTransaction(ctx context.Context, transaction Transaction) (Transaction, error) {
	transactions, err := r.UpsertTransactions(ctx, []Transaction{transaction})
	if err != nil {
		return Transaction{}, err
	}
	return transactions[0], nil
}

func (r *Repository) UpsertTransactions(ctx context.Context, transactions []Transaction) ([]Transaction, error) {
	if len(transactions) == 0 {
		return nil, nil
	}

	stored := make([]Transaction, 0, len(transactions))
	for _, transaction := range transactions {
		existingID, err := r.findTransactionID(ctx, transaction.UserID, transaction.SourceImportID, transaction.Fingerprint)
		if err != nil {
			return nil, err
		}
		if existingID != "" {
			transaction.ID = existingID
		}

		row := r.pool.QueryRow(ctx, `
INSERT INTO transactions (
    id,
    user_id,
    account_id,
    source_import_id,
    fingerprint,
    merchant,
    category,
    amount_cents,
    currency,
    occurred_at,
    raw_line
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
)
ON CONFLICT (id) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    account_id = EXCLUDED.account_id,
    source_import_id = EXCLUDED.source_import_id,
    fingerprint = EXCLUDED.fingerprint,
    merchant = EXCLUDED.merchant,
    category = EXCLUDED.category,
    amount_cents = EXCLUDED.amount_cents,
    currency = EXCLUDED.currency,
    occurred_at = EXCLUDED.occurred_at,
    raw_line = EXCLUDED.raw_line,
    updated_at = now()
RETURNING id, user_id, account_id, source_import_id, fingerprint, merchant, category, amount_cents, currency, occurred_at, raw_line, created_at
`,
			transaction.ID,
			transaction.UserID,
			transaction.AccountID,
			transaction.SourceImportID,
			transaction.Fingerprint,
			transaction.Merchant,
			transaction.Category,
			transaction.AmountCents,
			transaction.Currency,
			transaction.OccurredAt,
			transaction.RawLine,
		)

		var storedTransaction Transaction
		if err := row.Scan(
			&storedTransaction.ID,
			&storedTransaction.UserID,
			&storedTransaction.AccountID,
			&storedTransaction.SourceImportID,
			&storedTransaction.Fingerprint,
			&storedTransaction.Merchant,
			&storedTransaction.Category,
			&storedTransaction.AmountCents,
			&storedTransaction.Currency,
			&storedTransaction.OccurredAt,
			&storedTransaction.RawLine,
			&storedTransaction.CreatedAt,
		); err != nil {
			return nil, err
		}
		stored = append(stored, storedTransaction)
	}
	if err := r.ensureCategories(ctx, stored); err != nil {
		return nil, err
	}
	return stored, nil
}

func (r *Repository) findTransactionID(ctx context.Context, userID, sourceImportID, fingerprint string) (string, error) {
	var id string
	err := r.pool.QueryRow(ctx, `
SELECT id
FROM transactions
WHERE user_id = $1 AND source_import_id = $2 AND fingerprint = $3
`,
		userID,
		sourceImportID,
		fingerprint,
	).Scan(&id)
	if err == nil {
		return id, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return "", err
}

func (r *Repository) ListTransactions(ctx context.Context, userID string, limit int) ([]Transaction, error) {
	if limit <= 0 {
		limit = 200
	}
	if strings.TrimSpace(userID) != "" {
		rows, err := r.pool.Query(ctx, `
SELECT id, user_id, account_id, source_import_id, fingerprint, merchant, category, amount_cents, currency, occurred_at, raw_line, created_at
FROM transactions
WHERE user_id = $1
ORDER BY occurred_at DESC, created_at DESC
LIMIT $2
`, userID, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanTransactions(rows, limit)
	}

	rows, err := r.pool.Query(ctx, `
SELECT id, user_id, account_id, source_import_id, fingerprint, merchant, category, amount_cents, currency, occurred_at, raw_line, created_at
FROM transactions
ORDER BY occurred_at DESC, created_at DESC
LIMIT $1
`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanTransactions(rows, limit)
}

func scanTransactions(rows pgx.Rows, limit int) ([]Transaction, error) {

	transactions := make([]Transaction, 0, limit)
	for rows.Next() {
		var transaction Transaction
		if err := rows.Scan(
			&transaction.ID,
			&transaction.UserID,
			&transaction.AccountID,
			&transaction.SourceImportID,
			&transaction.Fingerprint,
			&transaction.Merchant,
			&transaction.Category,
			&transaction.AmountCents,
			&transaction.Currency,
			&transaction.OccurredAt,
			&transaction.RawLine,
			&transaction.CreatedAt,
		); err != nil {
			return nil, err
		}
		transactions = append(transactions, transaction)
	}
	return transactions, rows.Err()
}

func (r *Repository) ListCategories(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT name FROM categories ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	categories := make([]string, 0, len(DefaultCategories))
	for rows.Next() {
		var category string
		if err := rows.Scan(&category); err != nil {
			return nil, err
		}
		categories = append(categories, category)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return categories, nil
}

func (r *Repository) ensureCategories(ctx context.Context, transactions []Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	names := make([]string, 0, len(transactions))
	for _, transaction := range transactions {
		if transaction.Category == "" || slices.Contains(names, transaction.Category) {
			continue
		}
		names = append(names, transaction.Category)
	}
	if len(names) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, name := range names {
		batch.Queue(`
INSERT INTO categories (name, kind)
VALUES ($1, 'derived')
ON CONFLICT (name) DO NOTHING
`, name)
	}
	results := r.pool.SendBatch(ctx, batch)
	for range names {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	return results.Close()
}

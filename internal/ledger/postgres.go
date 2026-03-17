package ledger

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

type dbtx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) UpsertTransaction(ctx context.Context, transaction Transaction) (Transaction, error) {
	transactions, err := r.UpsertTransactions(ctx, []Transaction{transaction})
	if err != nil {
		return Transaction{}, err
	}
	return transactions[0], nil
}

func (r *Repository) UpsertTransactions(ctx context.Context, transactions []Transaction) ([]Transaction, error) {
	return r.upsertTransactions(ctx, r.pool, transactions)
}

func (r *Repository) UpsertTransactionWithOutbox(ctx context.Context, transaction Transaction, topic string) (Transaction, error) {
	transactions, err := r.UpsertTransactionsWithOutbox(ctx, []Transaction{transaction}, topic)
	if err != nil {
		return Transaction{}, err
	}
	return transactions[0], nil
}

func (r *Repository) UpsertTransactionsWithOutbox(ctx context.Context, transactions []Transaction, topic string) ([]Transaction, error) {
	if len(transactions) == 0 {
		return nil, nil
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	stored, err := r.upsertTransactions(ctx, tx, transactions)
	if err != nil {
		return nil, err
	}
	if err := r.enqueueOutboxEvents(ctx, tx, topic, stored); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return stored, nil
}

func (r *Repository) upsertTransactions(ctx context.Context, db dbtx, transactions []Transaction) ([]Transaction, error) {
	if len(transactions) == 0 {
		return nil, nil
	}

	stored := make([]Transaction, 0, len(transactions))
	for _, transaction := range transactions {
		existingID, err := r.findTransactionID(ctx, db, transaction.UserID, transaction.SourceImportID, transaction.Fingerprint)
		if err != nil {
			return nil, err
		}
		if existingID != "" {
			transaction.ID = existingID
		}

		row := db.QueryRow(ctx, `
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
	if err := r.ensureCategories(ctx, db, stored); err != nil {
		return nil, err
	}
	return stored, nil
}

func (r *Repository) findTransactionID(ctx context.Context, db dbtx, userID, sourceImportID, fingerprint string) (string, error) {
	var id string
	err := db.QueryRow(ctx, `
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

func (r *Repository) ClaimPendingOutboxEvents(ctx context.Context, limit int, owner string, claimTTL time.Duration) ([]OutboxEvent, error) {
	if limit <= 0 {
		limit = 1
	}
	if claimTTL <= 0 {
		claimTTL = 30 * time.Second
	}

	rows, err := r.pool.Query(ctx, `
WITH candidates AS (
    SELECT id
    FROM outbox_events
    WHERE published_at IS NULL
      AND (claim_expires_at IS NULL OR claim_expires_at < now())
    ORDER BY created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events AS o
SET claim_owner = $2,
    claimed_at = now(),
    claim_expires_at = now() + ($3::bigint * interval '1 millisecond'),
    publish_attempts = o.publish_attempts + 1
FROM candidates
WHERE o.id = candidates.id
RETURNING o.id, o.topic, o.message_key, o.event_type, o.payload, o.publish_attempts, o.last_error, o.claim_owner, o.claimed_at, o.claim_expires_at, o.created_at, o.published_at
`, limit, strings.TrimSpace(owner), claimTTL.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(
			&event.ID,
			&event.Topic,
			&event.MessageKey,
			&event.EventType,
			&event.Payload,
			&event.PublishAttempts,
			&event.LastError,
			&event.ClaimOwner,
			&event.ClaimedAt,
			&event.ClaimExpiresAt,
			&event.CreatedAt,
			&event.PublishedAt,
		); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *Repository) MarkOutboxEventPublished(ctx context.Context, eventID string) error {
	_, err := r.pool.Exec(ctx, `
UPDATE outbox_events
SET published_at = now(),
    last_error = '',
    claim_owner = NULL,
    claimed_at = NULL,
    claim_expires_at = NULL
WHERE id = $1
`, strings.TrimSpace(eventID))
	return err
}

func (r *Repository) ReleaseOutboxEventClaim(ctx context.Context, eventID, lastError string) error {
	_, err := r.pool.Exec(ctx, `
UPDATE outbox_events
SET last_error = $2,
    claim_owner = NULL,
    claimed_at = NULL,
    claim_expires_at = NULL
WHERE id = $1
`, strings.TrimSpace(eventID), strings.TrimSpace(lastError))
	return err
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

func (r *Repository) ensureCategories(ctx context.Context, db dbtx, transactions []Transaction) error {
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
	results := db.SendBatch(ctx, batch)
	for range names {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	return results.Close()
}

func (r *Repository) enqueueOutboxEvents(ctx context.Context, db dbtx, topic string, transactions []Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, transaction := range transactions {
		event, err := NewTransactionOutboxEvent(topic, transaction)
		if err != nil {
			return err
		}
		batch.Queue(`
INSERT INTO outbox_events (
    id,
    topic,
    message_key,
    event_type,
    payload
) VALUES (
    $1, $2, $3, $4, $5::jsonb
)
ON CONFLICT (id) DO NOTHING
`, event.ID, event.Topic, event.MessageKey, event.EventType, []byte(event.Payload))
	}

	results := db.SendBatch(ctx, batch)
	for range transactions {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	return results.Close()
}

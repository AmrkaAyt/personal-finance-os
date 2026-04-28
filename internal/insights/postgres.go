package insights

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) Upsert(ctx context.Context, action Action) (Action, error) {
	normalized, err := Normalize(action)
	if err != nil {
		return Action{}, err
	}
	metadata, err := json.Marshal(normalized.Metadata)
	if err != nil {
		return Action{}, err
	}

	row := r.pool.QueryRow(ctx, `
insert into insight_actions (
    id,
    user_id,
    insight_id,
    insight_type,
    action,
    reason,
    snoozed_until,
    metadata
) values ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
on conflict (user_id, insight_id) do update set
    insight_type = excluded.insight_type,
    action = excluded.action,
    reason = excluded.reason,
    snoozed_until = excluded.snoozed_until,
    metadata = excluded.metadata,
    updated_at = now()
returning id, user_id, insight_id, insight_type, action, reason, snoozed_until, metadata, created_at, updated_at
`,
		normalized.ID,
		normalized.UserID,
		normalized.InsightID,
		normalized.InsightType,
		normalized.Action,
		normalized.Reason,
		normalized.SnoozedUntil,
		metadata,
	)
	return scanAction(row)
}

func (r *Repository) List(ctx context.Context, userID string, limit int) ([]Action, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	rows, err := r.pool.Query(ctx, `
select id, user_id, insight_id, insight_type, action, reason, snoozed_until, metadata, created_at, updated_at
from insight_actions
where user_id = $1
order by updated_at desc
limit $2
`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	actions := make([]Action, 0, limit)
	for rows.Next() {
		action, err := scanAction(rows)
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	return actions, rows.Err()
}

type actionRow interface {
	Scan(dest ...any) error
}

func scanAction(row actionRow) (Action, error) {
	var action Action
	var metadata []byte
	err := row.Scan(
		&action.ID,
		&action.UserID,
		&action.InsightID,
		&action.InsightType,
		&action.Action,
		&action.Reason,
		&action.SnoozedUntil,
		&metadata,
		&action.CreatedAt,
		&action.UpdatedAt,
	)
	if err != nil {
		return Action{}, err
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &action.Metadata); err != nil {
			return Action{}, err
		}
	}
	if action.Metadata == nil {
		action.Metadata = map[string]string{}
	}
	return action, nil
}

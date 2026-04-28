create table if not exists insight_actions (
    id text primary key,
    user_id text not null,
    insight_id text not null,
    insight_type text not null,
    action text not null,
    reason text not null default '',
    snoozed_until timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint insight_actions_user_insight_key unique (user_id, insight_id),
    constraint insight_actions_action_check check (
        action in (
            'acknowledge',
            'snooze',
            'resolve',
            'suppress_similar',
            'confirm_recurring',
            'reject_recurring',
            'recategorize'
        )
    ),
    constraint insight_actions_snooze_requires_until check (
        action <> 'snooze' or snoozed_until is not null
    )
);

create index if not exists idx_insight_actions_user_updated_at
    on insight_actions (user_id, updated_at desc);

create index if not exists idx_insight_actions_user_type_action
    on insight_actions (user_id, insight_type, action);

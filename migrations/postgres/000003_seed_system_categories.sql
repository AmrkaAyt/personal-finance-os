INSERT INTO categories (name, kind)
VALUES
    ('fees', 'system'),
    ('food', 'system'),
    ('groceries', 'system'),
    ('healthcare', 'system'),
    ('housing', 'system'),
    ('income', 'system'),
    ('pending', 'system'),
    ('subscriptions', 'system'),
    ('transfers', 'system'),
    ('transport', 'system'),
    ('travel', 'system'),
    ('uncategorized', 'system'),
    ('utilities', 'system')
ON CONFLICT (name) DO NOTHING;

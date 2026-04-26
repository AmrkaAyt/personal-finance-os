ALTER TABLE categories
    ADD COLUMN IF NOT EXISTS user_id text;

ALTER TABLE categories
    DROP CONSTRAINT IF EXISTS categories_pkey;

CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_system_name_unique
    ON categories (name)
    WHERE user_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_user_name_unique
    ON categories (user_id, name)
    WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_categories_user_kind_name
    ON categories (user_id, kind, name);

INSERT INTO categories (user_id, name, kind)
SELECT DISTINCT
    t.user_id,
    t.category,
    'derived'
FROM transactions AS t
WHERE t.user_id <> ''
  AND t.category <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM categories AS system_categories
      WHERE system_categories.user_id IS NULL
        AND system_categories.name = t.category
  )
ON CONFLICT DO NOTHING;

DELETE FROM categories
WHERE user_id IS NULL
  AND kind = 'derived';

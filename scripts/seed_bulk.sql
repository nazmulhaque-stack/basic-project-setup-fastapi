-- =============================================================================
-- High-Performance 1 Crore (10 Million / 1 Cr / 100 Lakh) Bulk Seeder
-- =============================================================================
-- Features:
--   - Collision-free sequence-based account numbers and unique emails
--   - Zero-overhead raw SQL generate_series execution
--   - Random distribution across departments, roles, and currencies
--
-- Usage:
--   To seed 1 Lakh (100k):     SELECT seed_diverse_users(100000);
--   To seed 10 Lakh (1M):      SELECT seed_diverse_users(1000000);
--   To seed 1 Crore (10M):     DO $$ BEGIN FOR i IN 1..10 LOOP PERFORM seed_diverse_users(1000000); END LOOP; END $$;
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SEQUENCE IF NOT EXISTS user_account_seq START 1000000;
CREATE SEQUENCE IF NOT EXISTS user_email_seq START 1000000;

-- Keep generated values ahead of data already present in the database. Sequence
-- increments from failed batches are also preserved, so never move a sequence
-- backward when resuming.
SELECT setval(
    'user_account_seq',
    GREATEST(
        (SELECT last_value FROM user_account_seq),
        COALESCE(
            (SELECT MAX(substring(account_number FROM '^ACC-([0-9]+)$')::bigint)
             FROM user_accounts),
            999999
        )
    ),
    true
);

SELECT setval(
    'user_email_seq',
    GREATEST(
        (SELECT last_value FROM user_email_seq),
        COALESCE(
            (SELECT MAX(substring(email FROM '^user_([0-9]+)@enterprise[.]io$')::bigint)
             FROM users),
            999999
        )
    ),
    true
);

CREATE OR REPLACE FUNCTION seed_diverse_users(batch_size INT)
RETURNS VOID AS $$
DECLARE
    dept_names TEXT[] := ARRAY['Engineering', 'Finance', 'Operations'];
    role_names TEXT[] := ARRAY['Administrator', 'Manager', 'Staff'];
    acc_types  TEXT[] := ARRAY['checking', 'savings', 'corporate', 'investment'];
    currencies TEXT[] := ARRAY['USD', 'EUR', 'GBP', 'JPY'];
    first_names TEXT[] := ARRAY['Liam', 'Olivia', 'Noah', 'Emma', 'Oliver', 'Charlotte', 'Elijah', 'Amelia', 'James', 'Ava', 'William', 'Sophia', 'Benjamin', 'Isabella', 'Lucas', 'Mia', 'Henry', 'Evelyn', 'Theodore', 'Harper', 'Nazmul', 'Sarah', 'Alex', 'Elena', 'Marcus'];
    last_names  TEXT[] := ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Haque', 'Connor', 'Vance', 'Reyes', 'Chen'];
BEGIN
    WITH generated_users AS (
        SELECT 
            gen_random_uuid() AS user_id,
            first_names[1 + floor(random() * array_length(first_names, 1))::int] || ' ' || 
            last_names[1 + floor(random() * array_length(last_names, 1))::int] AS full_name,
            'user_' || nextval('user_email_seq') || '@enterprise.io' AS user_email,
            role_names[1 + floor(random() * array_length(role_names, 1))::int] AS user_role,
            dept_names[1 + floor(random() * array_length(dept_names, 1))::int] AS user_dept,
            'enc_v1_' || encode(gen_random_bytes(24), 'base64') AS user_phone_enc,
            encode(digest('phone_' || gen_random_uuid()::text, 'sha256'), 'hex') AS user_blind_idx,
            now() - (random() * interval '730 days') AS user_created_at
        FROM generate_series(1, batch_size)
    ),
    inserted_users AS (
        INSERT INTO users (id, name, email, role, department, phone_encrypted, phone_blind_index, created_at)
        SELECT user_id, full_name, user_email, user_role, user_dept, user_phone_enc, user_blind_idx, user_created_at
        FROM generated_users
        RETURNING id, created_at
    )
    INSERT INTO user_accounts (id, user_id, account_number, account_type, balance_cents, currency, status, created_at, updated_at)
    SELECT 
        gen_random_uuid(),
        u.id,
        'ACC-' || lpad(nextval('user_account_seq')::text, 10, '0'),
        acc_types[1 + floor(random() * array_length(acc_types, 1))::int],
        (floor(random() * 50000000) + 100000)::bigint,
        currencies[1 + floor(random() * array_length(currencies, 1))::int],
        'active',
        u.created_at,
        u.created_at
    FROM inserted_users u;
END;
$$ LANGUAGE plpgsql;

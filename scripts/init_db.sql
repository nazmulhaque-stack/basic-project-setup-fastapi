-- =============================================================================
-- FastAPI High-Performance Backend: Database Initialization Script (DDL)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Departments Table
CREATE TABLE IF NOT EXISTS departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    code VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Roles Table
CREATE TABLE IF NOT EXISTS roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    permissions TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Users Table (Text/String relations, no strict FK constraints)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    role VARCHAR(50) NOT NULL DEFAULT 'user',
    department VARCHAR(100),
    phone_encrypted TEXT NOT NULL,
    phone_blind_index VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_users_email ON users (email);
CREATE INDEX IF NOT EXISTS ix_users_phone_blind_index ON users (phone_blind_index);
CREATE INDEX IF NOT EXISTS ix_users_role ON users (role);
CREATE INDEX IF NOT EXISTS ix_users_department ON users (department);
CREATE INDEX IF NOT EXISTS ix_users_created_at_id ON users (created_at, id);

-- 4. User Accounts Table
CREATE TABLE IF NOT EXISTS user_accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    account_number VARCHAR(64) NOT NULL UNIQUE,
    account_type VARCHAR(50) NOT NULL DEFAULT 'checking',
    balance_cents BIGINT NOT NULL DEFAULT 0,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_user_accounts_user_id ON user_accounts (user_id);
CREATE INDEX IF NOT EXISTS ix_user_accounts_account_number ON user_accounts (account_number);

-- 5. Transactional Outbox Table (Tracks ES Synchronization)
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox (status, created_at) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS ix_outbox_aggregate ON outbox (aggregate_type, aggregate_id);

-- Seed initial departments and roles
INSERT INTO departments (name, code, description) VALUES
('Engineering', 'ENG', 'Software Engineering & Platform Infrastructure'),
('Finance', 'FIN', 'Corporate Finance & Accounting'),
('Operations', 'OPS', 'Global Business Operations')
ON CONFLICT (name) DO NOTHING;

INSERT INTO roles (name, description, permissions) VALUES
('Administrator', 'System wide administration', ARRAY['users:read', 'users:write', 'system:admin']),
('Manager', 'Departmental management', ARRAY['users:read', 'reports:read']),
('Staff', 'Standard staff access', ARRAY['users:read'])
ON CONFLICT (name) DO NOTHING;

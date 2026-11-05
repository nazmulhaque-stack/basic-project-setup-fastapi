-- =============================================================================
-- FastAPI High-Performance Backend: Database Initialization Script (DDL)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Categories Table
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_categories_name ON categories (name);
CREATE INDEX IF NOT EXISTS ix_categories_created_at_id ON categories (created_at, id);

-- 2. Users Table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    role VARCHAR(50) NOT NULL,
    phone_encrypted TEXT NOT NULL,
    phone_blind_index VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_users_email ON users (email);
CREATE INDEX IF NOT EXISTS ix_users_phone_blind_index ON users (phone_blind_index);
CREATE INDEX IF NOT EXISTS ix_users_role ON users (role);
CREATE INDEX IF NOT EXISTS ix_users_created_at_id ON users (created_at, id);

-- 3. Products Table
CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sku VARCHAR(100) NOT NULL UNIQUE,
    price_cents INTEGER NOT NULL,
    category VARCHAR(100) NOT NULL,
    category_id INTEGER REFERENCES categories(id) ON DELETE RESTRICT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    stock INTEGER NOT NULL DEFAULT 0,
    reserved_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT reserved_quantity_lte_stock CHECK (reserved_quantity >= 0 AND reserved_quantity <= stock)
);

CREATE INDEX IF NOT EXISTS ix_products_sku ON products (sku);
CREATE INDEX IF NOT EXISTS ix_products_category ON products (category);
CREATE INDEX IF NOT EXISTS ix_products_category_id ON products (category_id);
CREATE INDEX IF NOT EXISTS ix_products_created_at_id ON products (created_at, id);

-- 4. Employees Table
CREATE TABLE IF NOT EXISTS employees (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INTEGER NOT NULL,
    location VARCHAR(255) NOT NULL,
    user_id UUID NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_employees_name ON employees (name);
CREATE INDEX IF NOT EXISTS ix_employees_location ON employees (location);
CREATE INDEX IF NOT EXISTS ix_employees_created_at_id ON employees (created_at, id);

-- 5. Orders Table
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    amount_cents INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_orders_customer_id ON orders (customer_id);
CREATE INDEX IF NOT EXISTS ix_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS ix_orders_created_at_id ON orders (created_at, id);

-- 6. Transactional Outbox Events Table
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    topic VARCHAR(249) NOT NULL,
    event_key VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS ix_outbox_events_unpublished ON outbox_events (published_at, created_at);
CREATE INDEX IF NOT EXISTS ix_outbox_events_aggregate_pending ON outbox_events (topic, event_key, published_at, created_at, id);

-- 7. Inventory Reservations Table
CREATE TABLE IF NOT EXISTS inventory_reservations (
    id UUID PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL UNIQUE,
    reservation_id VARCHAR(255) NOT NULL UNIQUE,
    order_id VARCHAR(255) NOT NULL,
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    location_id VARCHAR(255),
    status VARCHAR(16) NOT NULL CHECK (status IN ('reserved', 'rejected', 'expired')),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_inventory_reservations_product_id ON inventory_reservations (product_id);
CREATE INDEX IF NOT EXISTS ix_inventory_reservations_expiration ON inventory_reservations (status, expires_at);
CREATE INDEX IF NOT EXISTS ix_inventory_reservations_created_at ON inventory_reservations (created_at);

-- 8. Processed Events Table (for Consumer Idempotency)
CREATE TABLE IF NOT EXISTS processed_events (
    event_id VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
);


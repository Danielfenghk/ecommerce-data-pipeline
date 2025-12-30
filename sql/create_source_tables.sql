-- ============================================================
-- Source Database Tables for E-Commerce Data Pipeline
-- ============================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- CUSTOMERS TABLE
-- ============================================================
DROP TABLE IF EXISTS source_customers CASCADE;

CREATE TABLE source_customers (
    customer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    gender VARCHAR(10),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'USA',
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    email_verified BOOLEAN DEFAULT false,
    marketing_opt_in BOOLEAN DEFAULT false,
    customer_segment VARCHAR(50),
    lifetime_value DECIMAL(12, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for customers
CREATE INDEX idx_customers_email ON source_customers(email);
CREATE INDEX idx_customers_registration ON source_customers(registration_date);
CREATE INDEX idx_customers_segment ON source_customers(customer_segment);
CREATE INDEX idx_customers_active ON source_customers(is_active);
CREATE INDEX idx_customers_updated ON source_customers(updated_at);

-- ============================================================
-- PRODUCTS TABLE
-- ============================================================
DROP TABLE IF EXISTS source_products CASCADE;

CREATE TABLE source_products (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10, 2) CHECK (cost >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    reorder_level INTEGER DEFAULT 10,
    weight_kg DECIMAL(8, 3),
    dimensions_cm VARCHAR(50),
    color VARCHAR(50),
    size VARCHAR(50),
    material VARCHAR(100),
    supplier_id UUID,
    is_active BOOLEAN DEFAULT true,
    is_featured BOOLEAN DEFAULT false,
    rating DECIMAL(3, 2) DEFAULT 0 CHECK (rating >= 0 AND rating <= 5),
    review_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for products
CREATE INDEX idx_products_sku ON source_products(sku);
CREATE INDEX idx_products_category ON source_products(category);
CREATE INDEX idx_products_brand ON source_products(brand);
CREATE INDEX idx_products_price ON source_products(price);
CREATE INDEX idx_products_active ON source_products(is_active);
CREATE INDEX idx_products_updated ON source_products(updated_at);

-- ============================================================
-- ORDERS TABLE
-- ============================================================
DROP TABLE IF EXISTS source_orders CASCADE;

CREATE TABLE source_orders (
    order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_number VARCHAR(50) NOT NULL UNIQUE,
    customer_id UUID NOT NULL REFERENCES source_customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(50) DEFAULT 'pending',
    payment_status VARCHAR(50) DEFAULT 'pending',
    payment_method VARCHAR(50),
    shipping_method VARCHAR(50),
    subtotal DECIMAL(12, 2) NOT NULL CHECK (subtotal >= 0),
    tax_amount DECIMAL(12, 2) DEFAULT 0 CHECK (tax_amount >= 0),
    shipping_amount DECIMAL(12, 2) DEFAULT 0 CHECK (shipping_amount >= 0),
    discount_amount DECIMAL(12, 2) DEFAULT 0 CHECK (discount_amount >= 0),
    total_amount DECIMAL(12, 2) NOT NULL CHECK (total_amount >= 0),
    currency VARCHAR(3) DEFAULT 'USD',
    shipping_address_line1 VARCHAR(255),
    shipping_address_line2 VARCHAR(255),
    shipping_city VARCHAR(100),
    shipping_state VARCHAR(100),
    shipping_postal_code VARCHAR(20),
    shipping_country VARCHAR(100),
    billing_address_line1 VARCHAR(255),
    billing_address_line2 VARCHAR(255),
    billing_city VARCHAR(100),
    billing_state VARCHAR(100),
    billing_postal_code VARCHAR(20),
    billing_country VARCHAR(100),
    notes TEXT,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    refunded_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for orders
CREATE INDEX idx_orders_customer ON source_orders(customer_id);
CREATE INDEX idx_orders_date ON source_orders(order_date);
CREATE INDEX idx_orders_status ON source_orders(order_status);
CREATE INDEX idx_orders_payment_status ON source_orders(payment_status);
CREATE INDEX idx_orders_number ON source_orders(order_number);
CREATE INDEX idx_orders_updated ON source_orders(updated_at);

-- ============================================================
-- ORDER ITEMS TABLE
-- ============================================================
DROP TABLE IF EXISTS source_order_items CASCADE;

CREATE TABLE source_order_items (
    order_item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES source_orders(order_id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES source_products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount_percent DECIMAL(5, 2) DEFAULT 0 CHECK (discount_percent >= 0 AND discount_percent <= 100),
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    tax_amount DECIMAL(10, 2) DEFAULT 0,
    total_price DECIMAL(10, 2) NOT NULL CHECK (total_price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for order items
CREATE INDEX idx_order_items_order ON source_order_items(order_id);
CREATE INDEX idx_order_items_product ON source_order_items(product_id);
CREATE INDEX idx_order_items_updated ON source_order_items(updated_at);

-- ============================================================
-- INVENTORY TRANSACTIONS TABLE
-- ============================================================
DROP TABLE IF EXISTS source_inventory_transactions CASCADE;

CREATE TABLE source_inventory_transactions (
    transaction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id UUID NOT NULL REFERENCES source_products(product_id),
    transaction_type VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    previous_quantity INTEGER,
    new_quantity INTEGER,
    reference_type VARCHAR(50),
    reference_id UUID,
    notes TEXT,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for inventory transactions
CREATE INDEX idx_inv_trans_product ON source_inventory_transactions(product_id);
CREATE INDEX idx_inv_trans_type ON source_inventory_transactions(transaction_type);
CREATE INDEX idx_inv_trans_date ON source_inventory_transactions(created_at);

-- ============================================================
-- CUSTOMER EVENTS TABLE (for streaming data)
-- ============================================================
DROP TABLE IF EXISTS source_customer_events CASCADE;

CREATE TABLE source_customer_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID REFERENCES source_customers(customer_id),
    session_id VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    page_url VARCHAR(500),
    referrer_url VARCHAR(500),
    user_agent TEXT,
    ip_address INET,
    device_type VARCHAR(50),
    browser VARCHAR(50),
    os VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for customer events
CREATE INDEX idx_events_customer ON source_customer_events(customer_id);
CREATE INDEX idx_events_session ON source_customer_events(session_id);
CREATE INDEX idx_events_type ON source_customer_events(event_type);
CREATE INDEX idx_events_date ON source_customer_events(created_at);
CREATE INDEX idx_events_data ON source_customer_events USING gin(event_data);

-- ============================================================
-- PROMOTIONS TABLE
-- ============================================================
DROP TABLE IF EXISTS source_promotions CASCADE;

CREATE TABLE source_promotions (
    promotion_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    promotion_code VARCHAR(50) NOT NULL UNIQUE,
    promotion_name VARCHAR(255) NOT NULL,
    description TEXT,
    discount_type VARCHAR(50) NOT NULL,
    discount_value DECIMAL(10, 2) NOT NULL,
    minimum_order_amount DECIMAL(10, 2) DEFAULT 0,
    maximum_discount DECIMAL(10, 2),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    usage_limit INTEGER,
    usage_count INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    applies_to VARCHAR(50) DEFAULT 'all',
    category_ids UUID[],
    product_ids UUID[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for promotions
CREATE INDEX idx_promotions_code ON source_promotions(promotion_code);
CREATE INDEX idx_promotions_dates ON source_promotions(start_date, end_date);
CREATE INDEX idx_promotions_active ON source_promotions(is_active);

-- ============================================================
-- CATEGORIES TABLE
-- ============================================================
DROP TABLE IF EXISTS source_categories CASCADE;

CREATE TABLE source_categories (
    category_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    category_name VARCHAR(100) NOT NULL,
    parent_category_id UUID REFERENCES source_categories(category_id),
    description TEXT,
    image_url VARCHAR(500),
    display_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for categories
CREATE INDEX idx_categories_parent ON source_categories(parent_category_id);
CREATE INDEX idx_categories_active ON source_categories(is_active);

-- ============================================================
-- SUPPLIERS TABLE
-- ============================================================
DROP TABLE IF EXISTS source_suppliers CASCADE;

CREATE TABLE source_suppliers (
    supplier_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    supplier_name VARCHAR(255) NOT NULL,
    contact_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    payment_terms VARCHAR(50),
    lead_time_days INTEGER,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- TRIGGERS FOR UPDATED_AT
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON source_customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON source_products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON source_orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_order_items_updated_at
    BEFORE UPDATE ON source_order_items
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- ETL TRACKING TABLE
-- ============================================================
DROP TABLE IF EXISTS etl_job_tracking CASCADE;

CREATE TABLE etl_job_tracking (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name VARCHAR(100) NOT NULL,
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) DEFAULT 'running',
    rows_processed INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    rows_deleted INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_etl_job_name ON etl_job_tracking(job_name);
CREATE INDEX idx_etl_job_status ON etl_job_tracking(status);
CREATE INDEX idx_etl_job_start ON etl_job_tracking(start_time);
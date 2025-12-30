-- ============================================================
-- Data Warehouse Tables for E-Commerce Data Pipeline
-- Dimensional Model (Star Schema)
-- ============================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- DIMENSION: DATE
-- ============================================================
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for dim_date
CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_month ON dim_date(year, month);
CREATE INDEX idx_dim_date_quarter ON dim_date(year, quarter);

-- ============================================================
-- DIMENSION: TIME
-- ============================================================
DROP TABLE IF EXISTS dim_time CASCADE;

CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    full_time TIME NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    am_pm VARCHAR(2) NOT NULL,
    hour_12 INTEGER NOT NULL,
    time_period VARCHAR(20) NOT NULL,
    is_business_hour BOOLEAN NOT NULL
);

-- ============================================================
-- DIMENSION: CUSTOMERS
-- ============================================================
DROP TABLE IF EXISTS dim_customers CASCADE;

CREATE TABLE dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id UUID NOT NULL,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    age INTEGER,
    age_group VARCHAR(20),
    gender VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    region VARCHAR(50),
    registration_date DATE,
    registration_year INTEGER,
    registration_month INTEGER,
    tenure_months INTEGER,
    tenure_group VARCHAR(50),
    is_active BOOLEAN,
    email_verified BOOLEAN,
    marketing_opt_in BOOLEAN,
    customer_segment VARCHAR(50),
    lifetime_value DECIMAL(12, 2),
    ltv_tier VARCHAR(20),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(12, 2) DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    last_order_date DATE,
    days_since_last_order INTEGER,
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    rfm_segment VARCHAR(50),
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for dim_customers
CREATE INDEX idx_dim_cust_id ON dim_customers(customer_id);
CREATE INDEX idx_dim_cust_email ON dim_customers(email);
CREATE INDEX idx_dim_cust_segment ON dim_customers(customer_segment);
CREATE INDEX idx_dim_cust_current ON dim_customers(is_current);
CREATE INDEX idx_dim_cust_rfm ON dim_customers(rfm_segment);

-- ============================================================
-- DIMENSION: PRODUCTS
-- ============================================================
DROP TABLE IF EXISTS dim_products CASCADE;

CREATE TABLE dim_products (
    product_sk SERIAL PRIMARY KEY,
    product_id UUID NOT NULL,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    category_path VARCHAR(255),
    brand VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    price_tier VARCHAR(20),
    cost DECIMAL(10, 2),
    profit_margin DECIMAL(5, 2),
    margin_tier VARCHAR(20),
    stock_quantity INTEGER,
    stock_status VARCHAR(20),
    weight_kg DECIMAL(8, 3),
    color VARCHAR(50),
    size VARCHAR(50),
    material VARCHAR(100),
    is_active BOOLEAN,
    is_featured BOOLEAN,
    rating DECIMAL(3, 2),
    rating_tier VARCHAR(20),
    review_count INTEGER,
    popularity_score INTEGER,
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for dim_products
CREATE INDEX idx_dim_prod_id ON dim_products(product_id);
CREATE INDEX idx_dim_prod_sku ON dim_products(sku);
CREATE INDEX idx_dim_prod_category ON dim_products(category);
CREATE INDEX idx_dim_prod_brand ON dim_products(brand);
CREATE INDEX idx_dim_prod_current ON dim_products(is_current);

-- ============================================================
-- DIMENSION: GEOGRAPHY
-- ============================================================
DROP TABLE IF EXISTS dim_geography CASCADE;

CREATE TABLE dim_geography (
    geography_sk SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    region VARCHAR(50),
    sub_region VARCHAR(50),
    timezone VARCHAR(50),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    population INTEGER,
    median_income DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for dim_geography
CREATE INDEX idx_dim_geo_city ON dim_geography(city);
CREATE INDEX idx_dim_geo_state ON dim_geography(state);
CREATE INDEX idx_dim_geo_country ON dim_geography(country);
CREATE INDEX idx_dim_geo_region ON dim_geography(region);

-- ============================================================
-- DIMENSION: PAYMENT METHOD
-- ============================================================
DROP TABLE IF EXISTS dim_payment_method CASCADE;

CREATE TABLE dim_payment_method (
    payment_method_sk SERIAL PRIMARY KEY,
    payment_method_code VARCHAR(50) NOT NULL UNIQUE,
    payment_method_name VARCHAR(100) NOT NULL,
    payment_type VARCHAR(50),
    is_digital BOOLEAN,
    processing_fee_percent DECIMAL(5, 2),
    processing_fee_fixed DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- DIMENSION: SHIPPING METHOD
-- ============================================================
DROP TABLE IF EXISTS dim_shipping_method CASCADE;

CREATE TABLE dim_shipping_method (
    shipping_method_sk SERIAL PRIMARY KEY,
    shipping_method_code VARCHAR(50) NOT NULL UNIQUE,
    shipping_method_name VARCHAR(100) NOT NULL,
    carrier VARCHAR(100),
    delivery_days_min INTEGER,
    delivery_days_max INTEGER,
    base_cost DECIMAL(10, 2),
    cost_per_kg DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- DIMENSION: PROMOTIONS
-- ============================================================
DROP TABLE IF EXISTS dim_promotions CASCADE;

CREATE TABLE dim_promotions (
    promotion_sk SERIAL PRIMARY KEY,
    promotion_id UUID,
    promotion_code VARCHAR(50),
    promotion_name VARCHAR(255),
    discount_type VARCHAR(50),
    discount_value DECIMAL(10, 2),
    minimum_order_amount DECIMAL(10, 2),
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN,
    promotion_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Special row for no promotion
INSERT INTO dim_promotions (promotion_sk, promotion_code, promotion_name, discount_type, discount_value, is_active)
VALUES (0, 'NONE', 'No Promotion', 'none', 0, false);

-- ============================================================
-- FACT: ORDERS
-- ============================================================
DROP TABLE IF EXISTS fact_orders CASCADE;

CREATE TABLE fact_orders (
    order_sk SERIAL PRIMARY KEY,
    order_id UUID NOT NULL,
    order_number VARCHAR(50) NOT NULL,
    customer_sk INTEGER REFERENCES dim_customers(customer_sk),
    customer_id UUID NOT NULL,
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    geography_sk INTEGER REFERENCES dim_geography(geography_sk),
    payment_method_sk INTEGER REFERENCES dim_payment_method(payment_method_sk),
    shipping_method_sk INTEGER REFERENCES dim_shipping_method(shipping_method_sk),
    promotion_sk INTEGER REFERENCES dim_promotions(promotion_sk),
    order_status VARCHAR(50),
    payment_status VARCHAR(50),
    -- Measures
    item_count INTEGER,
    subtotal DECIMAL(12, 2),
    tax_amount DECIMAL(12, 2),
    shipping_amount DECIMAL(12, 2),
    discount_amount DECIMAL(12, 2),
    total_amount DECIMAL(12, 2),
    profit_amount DECIMAL(12, 2),
    -- Flags
    is_first_order BOOLEAN,
    is_repeat_customer BOOLEAN,
    has_promotion BOOLEAN,
    -- Timestamps
    order_timestamp TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    -- Processing metrics
    days_to_ship INTEGER,
    days_to_deliver INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fact_orders
CREATE INDEX idx_fact_orders_id ON fact_orders(order_id);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_sk);
CREATE INDEX idx_fact_orders_date ON fact_orders(date_key);
CREATE INDEX idx_fact_orders_status ON fact_orders(order_status);
CREATE INDEX idx_fact_orders_timestamp ON fact_orders(order_timestamp);

-- ============================================================
-- FACT: ORDER ITEMS
-- ============================================================
DROP TABLE IF EXISTS fact_order_items CASCADE;

CREATE TABLE fact_order_items (
    order_item_sk SERIAL PRIMARY KEY,
    order_item_id UUID NOT NULL,
    order_sk INTEGER REFERENCES fact_orders(order_sk),
    order_id UUID NOT NULL,
    product_sk INTEGER REFERENCES dim_products(product_sk),
    product_id UUID NOT NULL,
    customer_sk INTEGER REFERENCES dim_customers(customer_sk),
    date_key INTEGER REFERENCES dim_date(date_key),
    -- Measures
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    unit_cost DECIMAL(10, 2),
    discount_percent DECIMAL(5, 2),
    discount_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    total_price DECIMAL(10, 2),
    profit_amount DECIMAL(10, 2),
    profit_margin DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fact_order_items
CREATE INDEX idx_fact_oi_order ON fact_order_items(order_sk);
CREATE INDEX idx_fact_oi_product ON fact_order_items(product_sk);
CREATE INDEX idx_fact_oi_customer ON fact_order_items(customer_sk);
CREATE INDEX idx_fact_oi_date ON fact_order_items(date_key);

-- ============================================================
-- FACT: DAILY SALES SNAPSHOT
-- ============================================================
DROP TABLE IF EXISTS fact_daily_sales CASCADE;

CREATE TABLE fact_daily_sales (
    snapshot_sk SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    -- Order metrics
    total_orders INTEGER,
    total_items_sold INTEGER,
    unique_customers INTEGER,
    new_customers INTEGER,
    returning_customers INTEGER,
    -- Financial metrics
    gross_revenue DECIMAL(14, 2),
    net_revenue DECIMAL(14, 2),
    total_discounts DECIMAL(12, 2),
    total_tax DECIMAL(12, 2),
    total_shipping DECIMAL(12, 2),
    total_cost DECIMAL(14, 2),
    gross_profit DECIMAL(14, 2),
    -- Averages
    avg_order_value DECIMAL(10, 2),
    avg_items_per_order DECIMAL(6, 2),
    avg_discount_percent DECIMAL(5, 2),
    -- Order status counts
    pending_orders INTEGER,
    processing_orders INTEGER,
    shipped_orders INTEGER,
    delivered_orders INTEGER,
    cancelled_orders INTEGER,
    refunded_orders INTEGER,
    -- Conversion metrics
    cart_conversion_rate DECIMAL(5, 2),
    refund_rate DECIMAL(5, 2),
    cancellation_rate DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fact_daily_sales
CREATE UNIQUE INDEX idx_fact_daily_date ON fact_daily_sales(date_key);

-- ============================================================
-- FACT: INVENTORY SNAPSHOT
-- ============================================================
DROP TABLE IF EXISTS fact_inventory_snapshot CASCADE;

CREATE TABLE fact_inventory_snapshot (
    snapshot_sk SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    product_sk INTEGER REFERENCES dim_products(product_sk),
    -- Inventory metrics
    quantity_on_hand INTEGER,
    quantity_reserved INTEGER,
    quantity_available INTEGER,
    quantity_in_transit INTEGER,
    -- Financial metrics
    inventory_value DECIMAL(14, 2),
    inventory_cost DECIMAL(14, 2),
    -- Stock metrics
    days_of_supply INTEGER,
    turnover_rate DECIMAL(6, 2),
    is_low_stock BOOLEAN,
    is_out_of_stock BOOLEAN,
    is_overstock BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fact_inventory_snapshot
CREATE INDEX idx_fact_inv_date ON fact_inventory_snapshot(date_key);
CREATE INDEX idx_fact_inv_product ON fact_inventory_snapshot(product_sk);
CREATE UNIQUE INDEX idx_fact_inv_unique ON fact_inventory_snapshot(date_key, product_sk);

-- ============================================================
-- FACT: CUSTOMER EVENTS
-- ============================================================
DROP TABLE IF EXISTS fact_customer_events CASCADE;

CREATE TABLE fact_customer_events (
    event_sk SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    customer_sk INTEGER REFERENCES dim_customers(customer_sk),
    customer_id UUID,
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    session_id VARCHAR(100),
    event_type VARCHAR(50),
    event_category VARCHAR(50),
    page_url VARCHAR(500),
    referrer_url VARCHAR(500),
    product_sk INTEGER REFERENCES dim_products(product_sk),
    product_id UUID,
    device_type VARCHAR(50),
    browser VARCHAR(50),
    os VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(100),
    -- Measures
    page_load_time_ms INTEGER,
    time_on_page_seconds INTEGER,
    event_value DECIMAL(10, 2),
    event_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fact_customer_events
CREATE INDEX idx_fact_events_customer ON fact_customer_events(customer_sk);
CREATE INDEX idx_fact_events_date ON fact_customer_events(date_key);
CREATE INDEX idx_fact_events_type ON fact_customer_events(event_type);
CREATE INDEX idx_fact_events_session ON fact_customer_events(session_id);
CREATE INDEX idx_fact_events_timestamp ON fact_customer_events(event_timestamp);

-- ============================================================
-- DATA QUALITY METRICS TABLE
-- ============================================================
DROP TABLE IF EXISTS data_quality_metrics CASCADE;

CREATE TABLE data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL,
    overall_status VARCHAR(50),
    results_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dq_timestamp ON data_quality_metrics(run_timestamp);
CREATE INDEX idx_dq_status ON data_quality_metrics(overall_status);

-- ============================================================
-- POPULATE DIM_DATE
-- ============================================================
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    week, day_of_month, day_of_week, day_name, is_weekend,
    fiscal_year, fiscal_quarter, fiscal_month
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
    d::DATE as full_date,
    EXTRACT(YEAR FROM d)::INTEGER as year,
    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
    EXTRACT(MONTH FROM d)::INTEGER as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(WEEK FROM d)::INTEGER as week,
    EXTRACT(DAY FROM d)::INTEGER as day_of_month,
    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 
         THEN EXTRACT(YEAR FROM d)::INTEGER + 1 
         ELSE EXTRACT(YEAR FROM d)::INTEGER END as fiscal_year,
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 
         THEN ((EXTRACT(MONTH FROM d) - 7) / 3 + 1)::INTEGER
         ELSE ((EXTRACT(MONTH FROM d) + 5) / 3 + 1)::INTEGER END as fiscal_quarter,
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 
         THEN (EXTRACT(MONTH FROM d) - 6)::INTEGER
         ELSE (EXTRACT(MONTH FROM d) + 6)::INTEGER END as fiscal_month
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- ============================================================
-- POPULATE DIM_TIME
-- ============================================================
INSERT INTO dim_time (
    time_key, full_time, hour, minute, second,
    am_pm, hour_12, time_period, is_business_hour
)
SELECT
    (EXTRACT(HOUR FROM t) * 10000 + EXTRACT(MINUTE FROM t) * 100 + EXTRACT(SECOND FROM t))::INTEGER as time_key,
    t::TIME as full_time,
    EXTRACT(HOUR FROM t)::INTEGER as hour,
    EXTRACT(MINUTE FROM t)::INTEGER as minute,
    EXTRACT(SECOND FROM t)::INTEGER as second,
    CASE WHEN EXTRACT(HOUR FROM t) < 12 THEN 'AM' ELSE 'PM' END as am_pm,
    CASE WHEN EXTRACT(HOUR FROM t) = 0 THEN 12
         WHEN EXTRACT(HOUR FROM t) > 12 THEN (EXTRACT(HOUR FROM t) - 12)::INTEGER
         ELSE EXTRACT(HOUR FROM t)::INTEGER END as hour_12,
    CASE 
        WHEN EXTRACT(HOUR FROM t) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM t) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM t) BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END as time_period,
    EXTRACT(HOUR FROM t) BETWEEN 9 AND 17 as is_business_hour
FROM generate_series('2000-01-01 00:00:00'::TIMESTAMP, '2000-01-01 23:59:59'::TIMESTAMP, '1 second'::INTERVAL) t
ON CONFLICT (time_key) DO NOTHING;
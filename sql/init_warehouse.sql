-- Warehouse Database Initialization

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    registration_date DATE,
    customer_tenure_days INTEGER,
    is_current BOOLEAN DEFAULT TRUE,
    effective_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    current_price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    price_tier VARCHAR(50),
    is_current BOOLEAN DEFAULT TRUE,
    effective_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    subtotal DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_amount DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    item_count INTEGER,
    order_value_tier VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Sales Fact
CREATE TABLE IF NOT EXISTS fact_daily_sales (
    daily_sales_key SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    total_orders INTEGER,
    total_revenue DECIMAL(14,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL Job Log
CREATE TABLE IF NOT EXISTS etl_job_log (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(200),
    status VARCHAR(50),
    records_processed INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Results
CREATE TABLE IF NOT EXISTS data_quality_results (
    result_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_name VARCHAR(200),
    passed BOOLEAN,
    details JSONB,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate Date Dimension (2023-2025)
INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month,
                      week_of_year, month_number, month_name, quarter, year, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(DOW FROM d)::INTEGER,
    TO_CHAR(d, 'Day'),
    EXTRACT(DAY FROM d)::INTEGER,
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2023-01-01'::date, '2025-12-31'::date, '1 day'::interval) d
ON CONFLICT (date_key) DO NOTHING;

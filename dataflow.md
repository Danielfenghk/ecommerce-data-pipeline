Hands-On Guide: Making Data Flow Through Your Pipeline


Step 1: Verify All Services Are Running
Bash

# Check all containers are up
docker-compose ps
Expected output - all services should show "Up":

text

NAME                STATUS
kafka               Up
zookeeper           Up
postgres-source     Up
postgres-warehouse  Up
postgres-airflow    Up
spark-master        Up
spark-worker        Up
minio               Up
airflow-webserver   Up
airflow-scheduler   Up
grafana             Up
metabase            Up
redis               Up
Step 2: Load Sample Data into Source Database
2.1 Connect to Source Database
Bash

# Connect to source PostgreSQL
docker exec -it postgres-source psql -U ecommerce_user -d ecommerce_source
2.2 Create Tables and Load Data
Run these SQL commands inside psql:

SQL

-- ============================================
-- Create Source Tables
-- ============================================

-- Customers Table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    registration_date TIMESTAMP,
    last_login TIMESTAMP,
    customer_segment VARCHAR(50),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products Table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    stock_quantity INTEGER,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders Table
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    order_date TIMESTAMP,
    status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_address TEXT,
    subtotal DECIMAL(12,2),
    tax DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Items Table
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2) DEFAULT 0,
    total_price DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Insert Sample Data
-- ============================================

-- Insert Customers
INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, country, postal_code, registration_date, last_login, customer_segment, total_orders, total_spent) VALUES
('CUST001', 'John', 'Smith', 'john.smith@email.com', '+1-555-0101', 'New York', 'NY', 'USA', '10001', '2023-06-15 10:30:00', '2024-01-20 14:00:00', 'Premium', 25, 2450.75),
('CUST002', 'Emily', 'Johnson', 'emily.j@email.com', '+1-555-0102', 'Los Angeles', 'CA', 'USA', '90001', '2023-07-20 14:45:00', '2024-01-22 09:30:00', 'Regular', 12, 890.50),
('CUST003', 'Michael', 'Williams', 'm.williams@email.com', '+1-555-0103', 'Chicago', 'IL', 'USA', '60601', '2023-08-10 09:00:00', '2024-01-25 16:15:00', 'Premium', 30, 3200.00),
('CUST004', 'Sarah', 'Brown', 'sarah.brown@email.com', '+1-555-0104', 'Houston', 'TX', 'USA', '77001', '2023-09-05 11:20:00', '2024-01-28 11:00:00', 'Regular', 8, 520.25),
('CUST005', 'David', 'Jones', 'david.jones@email.com', '+1-555-0105', 'Phoenix', 'AZ', 'USA', '85001', '2023-10-12 15:00:00', '2024-01-30 13:45:00', 'New', 3, 175.00),
('CUST006', 'Jessica', 'Garcia', 'jgarcia@email.com', '+1-555-0106', 'Philadelphia', 'PA', 'USA', '19101', '2023-11-08 08:30:00', '2024-02-01 10:30:00', 'Regular', 15, 1100.50),
('CUST007', 'Robert', 'Miller', 'r.miller@email.com', '+1-555-0107', 'San Antonio', 'TX', 'USA', '78201', '2023-12-01 12:15:00', '2024-02-03 15:00:00', 'New', 5, 350.75),
('CUST008', 'Jennifer', 'Davis', 'jdavis@email.com', '+1-555-0108', 'San Diego', 'CA', 'USA', '92101', '2024-01-05 10:00:00', '2024-02-05 12:30:00', 'Premium', 22, 2800.00),
('CUST009', 'William', 'Rodriguez', 'wrodriguez@email.com', '+1-555-0109', 'Dallas', 'TX', 'USA', '75201', '2024-01-10 14:30:00', '2024-02-08 09:00:00', 'Regular', 10, 750.25),
('CUST010', 'Amanda', 'Martinez', 'amartinez@email.com', '+1-555-0110', 'San Jose', 'CA', 'USA', '95101', '2024-01-15 09:45:00', '2024-02-10 14:15:00', 'New', 2, 125.50);

-- Insert Products
INSERT INTO products (product_id, name, category, subcategory, brand, price, cost, stock_quantity, description) VALUES
('PROD001', 'Wireless Bluetooth Headphones', 'Electronics', 'Audio', 'SoundMax', 79.99, 45.00, 150, 'Premium wireless headphones with noise cancellation'),
('PROD002', 'Organic Green Tea', 'Food & Beverages', 'Tea', 'NatureBrew', 12.99, 6.50, 500, '100% organic green tea leaves from Japan'),
('PROD003', 'Running Shoes Pro', 'Sports & Outdoors', 'Footwear', 'SpeedRunner', 129.99, 75.00, 200, 'Professional running shoes with cushioned sole'),
('PROD004', 'Stainless Steel Water Bottle', 'Home & Kitchen', 'Drinkware', 'EcoLife', 24.99, 12.00, 300, 'Insulated water bottle keeps drinks cold for 24 hours'),
('PROD005', 'Yoga Mat Premium', 'Sports & Outdoors', 'Fitness', 'ZenFit', 39.99, 18.00, 250, 'Non-slip yoga mat with carrying strap'),
('PROD006', 'Smart Watch Series 5', 'Electronics', 'Wearables', 'TechTime', 299.99, 180.00, 100, 'Feature-rich smartwatch with health monitoring'),
('PROD007', 'Organic Honey', 'Food & Beverages', 'Sweeteners', 'BeeNatural', 18.99, 9.00, 400, 'Pure organic honey from local beekeepers'),
('PROD008', 'Laptop Stand Adjustable', 'Electronics', 'Accessories', 'ErgoTech', 49.99, 25.00, 175, 'Ergonomic laptop stand with adjustable height'),
('PROD009', 'Cotton T-Shirt Classic', 'Clothing', 'Tops', 'ComfortWear', 19.99, 8.00, 600, '100% cotton classic fit t-shirt'),
('PROD010', 'Vitamin D3 Supplements', 'Health & Wellness', 'Vitamins', 'VitaHealth', 15.99, 7.50, 350, 'High-potency vitamin D3 supplements');

-- Insert Orders
INSERT INTO orders (order_id, customer_id, order_date, status, payment_method, shipping_address, subtotal, tax, shipping_cost, total_amount) VALUES
('ORD-2024-00001', 'CUST001', '2024-01-15 10:30:00', 'completed', 'credit_card', '123 Main St, New York, NY 10001', 129.97, 10.40, 5.99, 146.36),
('ORD-2024-00002', 'CUST003', '2024-01-16 14:45:00', 'completed', 'paypal', '789 Pine Rd, Chicago, IL 60601', 269.99, 21.60, 0, 291.59),
('ORD-2024-00003', 'CUST002', '2024-01-17 09:15:00', 'shipped', 'credit_card', '456 Oak Ave, Los Angeles, CA 90001', 169.98, 13.60, 7.99, 191.57),
('ORD-2024-00004', 'CUST005', '2024-01-18 16:00:00', 'processing', 'debit_card', '654 Maple Dr, Phoenix, AZ 85001', 76.95, 6.16, 5.99, 89.10),
('ORD-2024-00005', 'CUST008', '2024-01-19 11:30:00', 'completed', 'credit_card', '258 Walnut St, San Diego, CA 92101', 109.97, 8.80, 0, 118.77),
('ORD-2024-00006', 'CUST004', '2024-01-20 08:45:00', 'completed', 'paypal', '321 Elm St, Houston, TX 77001', 71.96, 5.76, 5.99, 83.71),
('ORD-2024-00007', 'CUST006', '2024-01-21 13:20:00', 'shipped', 'credit_card', '987 Cedar Ln, Philadelphia, PA 19101', 72.47, 5.80, 5.99, 84.26),
('ORD-2024-00008', 'CUST007', '2024-01-22 15:50:00', 'pending', 'debit_card', '147 Birch Way, San Antonio, TX 78201', 79.99, 6.40, 0, 86.39),
('ORD-2024-00009', 'CUST009', '2024-01-23 10:00:00', 'completed', 'credit_card', '369 Spruce Ave, Dallas, TX 75201', 70.95, 5.68, 5.99, 82.62),
('ORD-2024-00010', 'CUST010', '2024-01-24 12:30:00', 'processing', 'paypal', '741 Ash Blvd, San Jose, CA 95101', 71.99, 5.76, 5.99, 83.74);

-- Insert Order Items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount, total_price) VALUES
('ORD-2024-00001', 'PROD001', 1, 79.99, 0, 79.99),
('ORD-2024-00001', 'PROD004', 2, 24.99, 5.00, 44.98),
('ORD-2024-00002', 'PROD006', 1, 299.99, 30.00, 269.99),
('ORD-2024-00003', 'PROD003', 1, 129.99, 0, 129.99),
('ORD-2024-00003', 'PROD005', 1, 39.99, 0, 39.99),
('ORD-2024-00004', 'PROD002', 3, 12.99, 0, 38.97),
('ORD-2024-00004', 'PROD007', 2, 18.99, 0, 37.98),
('ORD-2024-00005', 'PROD008', 1, 49.99, 5.00, 44.99),
('ORD-2024-00005', 'PROD010', 2, 15.99, 0, 31.98),
('ORD-2024-00005', 'PROD009', 1, 19.99, 0, 19.99),
('ORD-2024-00006', 'PROD009', 4, 19.99, 8.00, 71.96),
('ORD-2024-00007', 'PROD010', 2, 15.99, 0, 31.98),
('ORD-2024-00007', 'PROD007', 1, 18.99, 0, 18.99),
('ORD-2024-00008', 'PROD001', 1, 79.99, 0, 79.99),
('ORD-2024-00009', 'PROD002', 2, 12.99, 0, 25.98),
('ORD-2024-00009', 'PROD004', 2, 24.99, 5.00, 44.98),
('ORD-2024-00010', 'PROD001', 1, 79.99, 8.00, 71.99);

-- Verify data loaded
SELECT 'Customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items', COUNT(*) FROM order_items;

-- Exit psql
\q
Step 3: Initialize Data Warehouse
3.1 Connect to Warehouse Database
Bash

docker exec -it postgres-warehouse psql -U warehouse_user -d ecommerce_warehouse
3.2 Create Warehouse Schema (Star Schema)
SQL

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Customer Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    customer_segment VARCHAR(50),
    registration_date DATE,
    customer_tenure_days INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    is_active BOOLEAN DEFAULT TRUE,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Geography Dimension
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    region VARCHAR(100),
    postal_code VARCHAR(20),
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    geography_key INTEGER REFERENCES dim_geography(geography_key),
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    subtotal DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(12,2),
    item_count INTEGER,
    is_first_order BOOLEAN DEFAULT FALSE,
    order_value_tier VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Order Items
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_key SERIAL PRIMARY KEY,
    order_key INTEGER REFERENCES fact_orders(order_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    discount_amount DECIMAL(10,2) DEFAULT 0,
    total_price DECIMAL(12,2),
    profit_amount DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Daily Sales (Aggregated)
CREATE TABLE IF NOT EXISTS fact_daily_sales (
    daily_sales_key SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    total_orders INTEGER,
    total_items_sold INTEGER,
    gross_sales DECIMAL(14,2),
    total_discounts DECIMAL(12,2),
    net_sales DECIMAL(14,2),
    total_tax DECIMAL(12,2),
    total_shipping DECIMAL(12,2),
    total_revenue DECIMAL(14,2),
    total_cost DECIMAL(14,2),
    total_profit DECIMAL(14,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INTEGER,
    new_customers INTEGER,
    returning_customers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DATA QUALITY TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS data_quality_results (
    result_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    table_name VARCHAR(100),
    check_name VARCHAR(200),
    check_type VARCHAR(50),
    passed BOOLEAN,
    actual_value DECIMAL(15,4),
    expected_threshold DECIMAL(15,4),
    severity VARCHAR(20),
    details JSONB,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_job_log (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(200),
    job_type VARCHAR(50),
    status VARCHAR(50),
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- POPULATE DATE DIMENSION (2023-2025)
-- ============================================

INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month, day_of_year, 
                      week_of_year, month_number, month_name, quarter, year, is_weekend, 
                      fiscal_year, fiscal_quarter)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
    d as full_date,
    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DAY FROM d)::INTEGER as day_of_month,
    EXTRACT(DOY FROM d)::INTEGER as day_of_year,
    EXTRACT(WEEK FROM d)::INTEGER as week_of_year,
    EXTRACT(MONTH FROM d)::INTEGER as month_number,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
    EXTRACT(YEAR FROM d)::INTEGER as year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    EXTRACT(YEAR FROM d)::INTEGER as fiscal_year,
    EXTRACT(QUARTER FROM d)::INTEGER as fiscal_quarter
FROM generate_series('2023-01-01'::date, '2025-12-31'::date, '1 day'::interval) d
ON CONFLICT (date_key) DO NOTHING;

-- Verify tables created
SELECT table_name, 
       (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
ORDER BY table_name;

-- Exit
\q
Step 4: Run ETL Pipeline Manually (Python)
4.1 Create a Python Script to Run ETL
Create a file run_etl_manual.py in your project root:

Python

#!/usr/bin/env python3
"""
Manual ETL Runner - Run this to process data through the pipeline
"""

import os
import sys
import pandas as pd
from datetime import datetime
from decimal import Decimal

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import psycopg2
from psycopg2.extras import execute_values

# Configuration
SOURCE_DB = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_source',
    'user': 'ecommerce_user',
    'password': 'ecommerce_pass'
}

WAREHOUSE_DB = {
    'host': 'localhost',
    'port': 5433,  # Note: different port!
    'database': 'ecommerce_warehouse',
    'user': 'warehouse_user',
    'password': 'warehouse_pass'
}


def get_connection(db_config):
    """Create database connection."""
    return psycopg2.connect(**db_config)


def extract_customers():
    """Extract customers from source."""
    print("📥 Extracting customers from source...")
    conn = get_connection(SOURCE_DB)
    query = "SELECT * FROM customers"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"   Extracted {len(df)} customers")
    return df


def extract_products():
    """Extract products from source."""
    print("📥 Extracting products from source...")
    conn = get_connection(SOURCE_DB)
    query = "SELECT * FROM products"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"   Extracted {len(df)} products")
    return df


def extract_orders():
    """Extract orders from source."""
    print("📥 Extracting orders from source...")
    conn = get_connection(SOURCE_DB)
    query = """
        SELECT o.*, 
               COUNT(oi.item_id) as item_count,
               SUM(oi.discount) as total_discount
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.customer_id, o.order_date, o.status, 
                 o.payment_method, o.shipping_address, o.subtotal, 
                 o.tax, o.shipping_cost, o.total_amount, 
                 o.created_at, o.updated_at
    """
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"   Extracted {len(df)} orders")
    return df


def extract_order_items():
    """Extract order items from source."""
    print("📥 Extracting order items from source...")
    conn = get_connection(SOURCE_DB)
    query = "SELECT * FROM order_items"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"   Extracted {len(df)} order items")
    return df


def transform_customers(df):
    """Transform customers for warehouse."""
    print("🔄 Transforming customers...")
    
    transformed = df.copy()
    
    # Create full name
    transformed['full_name'] = transformed['first_name'] + ' ' + transformed['last_name']
    
    # Calculate tenure
    transformed['customer_tenure_days'] = (
        datetime.now() - pd.to_datetime(transformed['registration_date'])
    ).dt.days
    
    # Add SCD columns
    transformed['effective_date'] = datetime.now().date()
    transformed['expiration_date'] = None
    transformed['is_current'] = True
    transformed['is_active'] = True
    
    print(f"   Transformed {len(transformed)} customers")
    return transformed


def transform_products(df):
    """Transform products for warehouse."""
    print("🔄 Transforming products...")
    
    transformed = df.copy()
    
    # Calculate profit margin
    transformed['profit_margin'] = (
        (transformed['price'] - transformed['cost']) / transformed['price'] * 100
    ).round(2)
    
    # Create price tier
    def get_price_tier(price):
        if price < 20:
            return 'Budget'
        elif price < 50:
            return 'Standard'
        elif price < 100:
            return 'Premium'
        else:
            return 'Luxury'
    
    transformed['price_tier'] = transformed['price'].apply(get_price_tier)
    transformed['current_price'] = transformed['price']
    
    # Add SCD columns
    transformed['effective_date'] = datetime.now().date()
    transformed['expiration_date'] = None
    transformed['is_current'] = True
    transformed['is_active'] = True
    
    print(f"   Transformed {len(transformed)} products")
    return transformed


def transform_orders(df):
    """Transform orders for warehouse."""
    print("🔄 Transforming orders...")
    
    transformed = df.copy()
    
    # Create date key
    transformed['date_key'] = pd.to_datetime(transformed['order_date']).dt.strftime('%Y%m%d').astype(int)
    
    # Create order value tier
    def get_value_tier(amount):
        if amount < 50:
            return 'Very Low'
        elif amount < 100:
            return 'Low'
        elif amount < 200:
            return 'Medium'
        elif amount < 500:
            return 'High'
        else:
            return 'Very High'
    
    transformed['order_value_tier'] = transformed['total_amount'].apply(get_value_tier)
    transformed['discount_amount'] = transformed['total_discount'].fillna(0)
    
    print(f"   Transformed {len(transformed)} orders")
    return transformed


def load_dim_customers(df):
    """Load customers to dimension table."""
    print("📤 Loading dim_customers...")
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    # Prepare data
    columns = ['customer_id', 'first_name', 'last_name', 'full_name', 'email', 
               'phone', 'city', 'state', 'country', 'postal_code', 
               'customer_segment', 'registration_date', 'customer_tenure_days',
               'is_active', 'effective_date', 'is_current']
    
    values = []
    for _, row in df.iterrows():
        values.append((
            row['customer_id'],
            row['first_name'],
            row['last_name'],
            row['full_name'],
            row['email'],
            row.get('phone'),
            row['city'],
            row['state'],
            row['country'],
            row['postal_code'],
            row['customer_segment'],
            row['registration_date'],
            int(row['customer_tenure_days']),
            True,
            datetime.now().date(),
            True
        ))
    
    # Insert with conflict handling
    insert_query = """
        INSERT INTO dim_customers 
        (customer_id, first_name, last_name, full_name, email, phone, city, state, 
         country, postal_code, customer_segment, registration_date, customer_tenure_days,
         is_active, effective_date, is_current)
        VALUES %s
        ON CONFLICT (customer_key) DO NOTHING
    """
    
    # Simple insert for new dimension
    for val in values:
        cur.execute("""
            INSERT INTO dim_customers 
            (customer_id, first_name, last_name, full_name, email, phone, city, state, 
             country, postal_code, customer_segment, registration_date, customer_tenure_days,
             is_active, effective_date, is_current)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, val)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   Loaded {len(values)} customers to warehouse")


def load_dim_products(df):
    """Load products to dimension table."""
    print("📤 Loading dim_products...")
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO dim_products 
            (product_id, name, category, subcategory, brand, current_price, cost,
             profit_margin, price_tier, is_active, effective_date, is_current)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row['product_id'],
            row['name'],
            row['category'],
            row['subcategory'],
            row['brand'],
            float(row['current_price']),
            float(row['cost']),
            float(row['profit_margin']),
            row['price_tier'],
            True,
            datetime.now().date(),
            True
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   Loaded {len(df)} products to warehouse")


def load_dim_geography(customers_df):
    """Load geography dimension."""
    print("📤 Loading dim_geography...")
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    # Get unique geographies
    geo_df = customers_df[['city', 'state', 'country', 'postal_code']].drop_duplicates()
    
    for _, row in geo_df.iterrows():
        cur.execute("""
            INSERT INTO dim_geography (city, state, country, postal_code, region)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row['city'],
            row['state'],
            row['country'],
            row['postal_code'],
            'North America'  # Simplified
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   Loaded {len(geo_df)} geographies to warehouse")


def load_fact_orders(orders_df, customers_df):
    """Load orders to fact table."""
    print("📤 Loading fact_orders...")
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    # Get customer key mapping
    cur.execute("SELECT customer_key, customer_id FROM dim_customers WHERE is_current = TRUE")
    customer_map = {row[1]: row[0] for row in cur.fetchall()}
    
    # Get geography key mapping
    cur.execute("SELECT geography_key, city, state FROM dim_geography")
    geo_map = {(row[1], row[2]): row[0] for row in cur.fetchall()}
    
    loaded = 0
    for _, row in orders_df.iterrows():
        customer_key = customer_map.get(row['customer_id'])
        
        # Get customer city/state for geography
        cust = customers_df[customers_df['customer_id'] == row['customer_id']]
        if len(cust) > 0:
            geo_key = geo_map.get((cust.iloc[0]['city'], cust.iloc[0]['state']))
        else:
            geo_key = None
        
        if customer_key:
            cur.execute("""
                INSERT INTO fact_orders 
                (order_id, customer_key, date_key, geography_key, order_status,
                 payment_method, subtotal, tax_amount, shipping_amount, 
                 discount_amount, total_amount, item_count, order_value_tier)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                row['order_id'],
                customer_key,
                int(row['date_key']),
                geo_key,
                row['status'],
                row['payment_method'],
                float(row['subtotal']),
                float(row['tax']),
                float(row['shipping_cost']),
                float(row['discount_amount']),
                float(row['total_amount']),
                int(row['item_count']),
                row['order_value_tier']
            ))
            loaded += 1
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   Loaded {loaded} orders to warehouse")


def calculate_daily_aggregates():
    """Calculate and load daily sales aggregates."""
    print("📊 Calculating daily aggregates...")
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO fact_daily_sales 
        (date_key, total_orders, total_items_sold, gross_sales, total_discounts,
         net_sales, total_tax, total_shipping, total_revenue, avg_order_value, unique_customers)
        SELECT 
            date_key,
            COUNT(*) as total_orders,
            SUM(item_count) as total_items_sold,
            SUM(subtotal) as gross_sales,
            SUM(discount_amount) as total_discounts,
            SUM(subtotal - discount_amount) as net_sales,
            SUM(tax_amount) as total_tax,
            SUM(shipping_amount) as total_shipping,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_key) as unique_customers
        FROM fact_orders
        GROUP BY date_key
        ON CONFLICT DO NOTHING
    """)
    
    conn.commit()
    
    # Get count
    cur.execute("SELECT COUNT(*) FROM fact_daily_sales")
    count = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    print(f"   Created {count} daily aggregate records")


def log_etl_job(job_name, status, records_processed, start_time, error=None):
    """Log ETL job execution."""
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    end_time = datetime.now()
    duration = int((end_time - start_time).total_seconds())
    
    cur.execute("""
        INSERT INTO etl_job_log 
        (job_name, job_type, status, records_processed, start_time, end_time, 
         duration_seconds, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job_name,
        'batch',
        status,
        records_processed,
        start_time,
        end_time,
        duration,
        error
    ))
    
    conn.commit()
    cur.close()
    conn.close()


def run_full_etl():
    """Run the complete ETL pipeline."""
    print("\n" + "="*60)
    print("🚀 Starting E-Commerce ETL Pipeline")
    print("="*60 + "\n")
    
    start_time = datetime.now()
    total_records = 0
    
    try:
        # EXTRACT
        print("\n📥 EXTRACT PHASE")
        print("-"*40)
        customers_df = extract_customers()
        products_df = extract_products()
        orders_df = extract_orders()
        order_items_df = extract_order_items()
        
        total_records = len(customers_df) + len(products_df) + len(orders_df) + len(order_items_df)
        
        # TRANSFORM
        print("\n🔄 TRANSFORM PHASE")
        print("-"*40)
        customers_transformed = transform_customers(customers_df)
        products_transformed = transform_products(products_df)
        orders_transformed = transform_orders(orders_df)
        
        # LOAD
        print("\n📤 LOAD PHASE")
        print("-"*40)
        load_dim_customers(customers_transformed)
        load_dim_products(products_transformed)
        load_dim_geography(customers_df)
        load_fact_orders(orders_transformed, customers_df)
        
        # AGGREGATE
        print("\n📊 AGGREGATION PHASE")
        print("-"*40)
        calculate_daily_aggregates()
        
        # Log success
        log_etl_job('full_etl_pipeline', 'SUCCESS', total_records, start_time)
        
        print("\n" + "="*60)
        print("✅ ETL Pipeline Completed Successfully!")
        print(f"   Total records processed: {total_records}")
        print(f"   Duration: {(datetime.now() - start_time).total_seconds():.2f} seconds")
        print("="*60 + "\n")
        
    except Exception as e:
        log_etl_job('full_etl_pipeline', 'FAILED', total_records, start_time, str(e))
        print(f"\n❌ ETL Pipeline Failed: {e}")
        raise


if __name__ == '__main__':
    run_full_etl()
4.2 Run the ETL Pipeline
Bash

# Make sure you're in the project directory
cd ecommerce-data-pipeline

# Install required packages (if not already)
pip install pandas psycopg2-binary

# Run the ETL
python run_etl_manual.py
Expected output:

text

============================================================
🚀 Starting E-Commerce ETL Pipeline
============================================================

📥 EXTRACT PHASE
----------------------------------------
📥 Extracting customers from source...
   Extracted 10 customers
📥 Extracting products from source...
   Extracted 10 products
📥 Extracting orders from source...
   Extracted 10 orders
📥 Extracting order items from source...
   Extracted 17 order items

🔄 TRANSFORM PHASE
----------------------------------------
🔄 Transforming customers...
   Transformed 10 customers
🔄 Transforming products...
   Transformed 10 products
🔄 Transforming orders...
   Transformed 10 orders

📤 LOAD PHASE
----------------------------------------
📤 Loading dim_customers...
   Loaded 10 customers to warehouse
📤 Loading dim_products...
   Loaded 10 products to warehouse
📤 Loading dim_geography...
   Loaded 10 geographies to warehouse
📤 Loading fact_orders...
   Loaded 10 orders to warehouse

📊 AGGREGATION PHASE
----------------------------------------
📊 Calculating daily aggregates...
   Created 10 daily aggregate records

============================================================
✅ ETL Pipeline Completed Successfully!
   Total records processed: 47
   Duration: 1.25 seconds
============================================================
Step 5: Verify Data in Warehouse
Bash

# Connect to warehouse
docker exec -it postgres-warehouse psql -U warehouse_user -d ecommerce_warehouse
Run these queries:

SQL

-- Check dimension tables
SELECT 'dim_customers' as table_name, COUNT(*) as rows FROM dim_customers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL SELECT 'fact_daily_sales', COUNT(*) FROM fact_daily_sales;

-- Sample customer data
SELECT customer_key, customer_id, full_name, customer_segment, customer_tenure_days
FROM dim_customers
LIMIT 5;

-- Sample product data
SELECT product_key, product_id, name, category, current_price, profit_margin, price_tier
FROM dim_products
LIMIT 5;

-- Sample order data
SELECT fo.order_id, dc.full_name, fo.total_amount, fo.order_status, fo.order_value_tier
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
LIMIT 5;

-- Daily sales summary
SELECT 
    dd.full_date,
    fds.total_orders,
    fds.total_revenue,
    fds.avg_order_value,
    fds.unique_customers
FROM fact_daily_sales fds
JOIN dim_date dd ON fds.date_key = dd.date_key
ORDER BY dd.full_date;

-- ETL job log
SELECT job_name, status, records_processed, duration_seconds, created_at
FROM etl_job_log
ORDER BY created_at DESC;

\q
Step 6: Setup Kafka and Send Real-Time Events
6.1 Create Kafka Topics
Bash

# Create topics
docker exec -it kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic orders \
    --if-not-exists

docker exec -it kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic events \
    --if-not-exists

# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
6.2 Create Kafka Event Producer Script
Create kafka_event_producer.py:

Python

#!/usr/bin/env python3
"""
Kafka Event Producer - Simulates real-time e-commerce events
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Sample data
CUSTOMER_IDS = [f'CUST{i:03d}' for i in range(1, 11)]
PRODUCT_IDS = [f'PROD{i:03d}' for i in range(1, 11)]
EVENT_TYPES = ['page_view', 'add_to_cart', 'remove_from_cart', 'purchase', 'search']
PAGES = ['home', 'product_list', 'product_detail', 'cart', 'checkout', 'confirmation']


def create_producer():
    """Create Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )


def generate_event():
    """Generate a random e-commerce event."""
    event_type = random.choice(EVENT_TYPES)
    
    event = {
        'event_id': f"EVT-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
        'event_type': event_type,
        'timestamp': datetime.now().isoformat(),
        'customer_id': random.choice(CUSTOMER_IDS),
        'session_id': f"SES-{random.randint(10000, 99999)}",
        'device': random.choice(['desktop', 'mobile', 'tablet']),
        'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
    }
    
    if event_type == 'page_view':
        event['page'] = random.choice(PAGES)
        event['referrer'] = random.choice(['google', 'facebook', 'direct', 'email'])
        
    elif event_type in ['add_to_cart', 'remove_from_cart']:
        event['product_id'] = random.choice(PRODUCT_IDS)
        event['quantity'] = random.randint(1, 5)
        event['price'] = round(random.uniform(10, 200), 2)
        
    elif event_type == 'purchase':
        event['order_id'] = f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        event['total_amount'] = round(random.uniform(20, 500), 2)
        event['payment_method'] = random.choice(['credit_card', 'paypal', 'debit_card'])
        
    elif event_type == 'search':
        event['search_query'] = random.choice(['headphones', 'running shoes', 'laptop', 'tea', 'watch'])
        event['results_count'] = random.randint(0, 100)
    
    return event


def produce_events(num_events=100, delay=0.5):
    """Produce events to Kafka."""
    producer = create_producer()
    
    print(f"\n🚀 Starting Kafka Event Producer")
    print(f"   Sending {num_events} events to topic 'events'")
    print(f"   Delay between events: {delay}s")
    print("-" * 50)
    
    for i in range(num_events):
        event = generate_event()
        
        # Send to Kafka
        producer.send(
            topic='events',
            key=event['customer_id'],
            value=event
        )
        
        print(f"[{i+1}/{num_events}] Sent {event['event_type']:15} | Customer: {event['customer_id']} | {event['event_id']}")
        
        time.sleep(delay)
    
    producer.flush()
    producer.close()
    
    print("-" * 50)
    print(f"✅ Sent {num_events} events successfully!")


if __name__ == '__main__':
    # Send 20 events with 0.5 second delay
    produce_events(num_events=20, delay=0.5)
6.3 Run Kafka Producer
Bash

# Install kafka-python if needed
pip install kafka-python

# Run the producer
python kafka_event_producer.py
6.4 Consume Events (Verify)
In a new terminal:

Bash

# Consume messages from the topic
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic events \
    --from-beginning
Step 7: Configure Airflow DAGs
7.1 Access Airflow UI
Go to http://localhost:8081
Login: admin / admin
7.2 Enable DAGs
In the Airflow UI:

Click on "DAGs" in the top menu
You should see your DAGs (if properly mounted)
Toggle the DAG switches to "ON" to enable them
7.3 Trigger DAG Manually
Click on a DAG name (e.g., daily_etl_pipeline)
Click the "Trigger DAG" button (play icon)
Watch the progress in the Graph view
Step 8: Setup MinIO Data Lake
8.1 Access MinIO Console
Go to http://localhost:9001
Login: minioadmin / minioadmin123
8.2 Create Buckets
In the MinIO Console:

Click "Buckets" → "Create Bucket"
Create these buckets:
raw-data
processed-data
analytics
8.3 Upload Data to Data Lake
Create upload_to_datalake.py:

Python

#!/usr/bin/env python3
"""
Upload data to MinIO Data Lake
"""

import os
import json
import pandas as pd
from datetime import datetime
from minio import Minio
from io import BytesIO

# MinIO configuration
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'


def get_minio_client():
    """Create MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def ensure_buckets(client):
    """Ensure required buckets exist."""
    buckets = ['raw-data', 'processed-data', 'analytics']
    for bucket in buckets:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"✅ Created bucket: {bucket}")
        else:
            print(f"📁 Bucket exists: {bucket}")


def upload_raw_data():
    """Upload raw data to data lake."""
    client = get_minio_client()
    ensure_buckets(client)
    
    timestamp = datetime.now().strftime('%Y/%m/%d/%H%M%S')
    
    # Create sample raw data
    orders = [
        {
            "order_id": f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}-{i}",
            "customer_id": f"CUST{(i % 10) + 1:03d}",
            "product_id": f"PROD{(i % 10) + 1:03d}",
            "quantity": (i % 5) + 1,
            "price": round(10 + (i * 5.5), 2),
            "timestamp": datetime.now().isoformat()
        }
        for i in range(1, 21)
    ]
    
    # Upload as JSON
    json_data = json.dumps(orders, indent=2).encode('utf-8')
    client.put_object(
        'raw-data',
        f'orders/{timestamp}/orders.json',
        BytesIO(json_data),
        len(json_data),
        content_type='application/json'
    )
    print(f"📤 Uploaded: raw-data/orders/{timestamp}/orders.json")
    
    # Upload as CSV
    df = pd.DataFrame(orders)
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    client.put_object(
        'raw-data',
        f'orders/{timestamp}/orders.csv',
        BytesIO(csv_data),
        len(csv_data),
        content_type='text/csv'
    )
    print(f"📤 Uploaded: raw-data/orders/{timestamp}/orders.csv")
    
    # Upload processed data (parquet-like format as CSV for simplicity)
    processed_df = df.copy()
    processed_df['processed_at'] = datetime.now().isoformat()
    processed_df['total_amount'] = processed_df['quantity'] * processed_df['price']
    
    processed_buffer = BytesIO()
    processed_df.to_csv(processed_buffer, index=False)
    processed_data = processed_buffer.getvalue()
    
    client.put_object(
        'processed-data',
        f'orders/{timestamp}/orders_processed.csv',
        BytesIO(processed_data),
        len(processed_data),
        content_type='text/csv'
    )
    print(f"📤 Uploaded: processed-data/orders/{timestamp}/orders_processed.csv")
    
    # Create analytics summary
    analytics = {
        "report_date": datetime.now().isoformat(),
        "total_orders": len(orders),
        "total_revenue": sum(o['price'] * o['quantity'] for o in orders),
        "avg_order_value": sum(o['price'] * o['quantity'] for o in orders) / len(orders),
        "unique_customers": len(set(o['customer_id'] for o in orders)),
        "unique_products": len(set(o['product_id'] for o in orders))
    }
    
    analytics_data = json.dumps(analytics, indent=2).encode('utf-8')
    client.put_object(
        'analytics',
        f'daily_summary/{timestamp}/summary.json',
        BytesIO(analytics_data),
        len(analytics_data),
        content_type='application/json'
    )
    print(f"📤 Uploaded: analytics/daily_summary/{timestamp}/summary.json")
    
    print("\n✅ All data uploaded to Data Lake successfully!")


def list_bucket_contents():
    """List contents of all buckets."""
    client = get_minio_client()
    
    print("\n📁 Data Lake Contents:")
    print("=" * 60)
    
    for bucket in ['raw-data', 'processed-data', 'analytics']:
        if client.bucket_exists(bucket):
            print(f"\n📂 {bucket}/")
            objects = client.list_objects(bucket, recursive=True)
            for obj in objects:
                print(f"   └── {obj.object_name} ({obj.size} bytes)")


if __name__ == '__main__':
    print("\n🚀 Uploading data to MinIO Data Lake")
    print("=" * 60)
    
    upload_raw_data()
    list_bucket_contents()
Run it:

Bash

pip install minio

python upload_to_datalake.py
Step 9: Setup Grafana Dashboards
9.1 Access Grafana
Go to http://localhost:3000
Login: admin / admin
9.2 Add Data Source
Go to Configuration → Data Sources
Click "Add data source"
Select "PostgreSQL"
Configure:
Name: Warehouse DB
Host: postgres-warehouse:5432
Database: ecommerce_warehouse
User: warehouse_user
Password: warehouse_pass
SSL Mode: disable
Click "Save & Test"
9.3 Create Dashboard
Click "+" → "Dashboard"
Click "Add new panel"
Use this SQL query for daily revenue:
SQL

SELECT 
    dd.full_date as time,
    fds.total_revenue as "Total Revenue",
    fds.total_orders as "Orders"
FROM fact_daily_sales fds
JOIN dim_date dd ON fds.date_key = dd.date_key
ORDER BY dd.full_date
Configure visualization (Time series)
Click "Apply"
Add more panels as needed
Step 10: Setup Metabase for BI
10.1 Access Metabase
Go to http://localhost:3001
Complete the setup wizard:
Create admin account
Select "I'll add my data later"
10.2 Connect to Warehouse
Go to Settings → Admin → Databases
Add database:
Type: PostgreSQL
Name: E-Commerce Warehouse
Host: postgres-warehouse
Port: 5432
Database: ecommerce_warehouse
Username: warehouse_user
Password: warehouse_pass
10.3 Explore Data
Click "New" → "Question"
Select "E-Commerce Warehouse"
Choose a table (e.g., fact_orders)
Build visualizations!
Step 11: Run Analytics Queries
Connect to warehouse and run business queries:

Bash

docker exec -it postgres-warehouse psql -U warehouse_user -d ecommerce_warehouse
SQL

-- Top customers by revenue
SELECT 
    dc.full_name,
    dc.customer_segment,
    COUNT(fo.order_key) as total_orders,
    SUM(fo.total_amount) as total_revenue,
    AVG(fo.total_amount) as avg_order_value
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
GROUP BY dc.customer_key, dc.full_name, dc.customer_segment
ORDER BY total_revenue DESC;

-- Product performance
SELECT 
    dp.name,
    dp.category,
    dp.price_tier,
    dp.profit_margin,
    COUNT(DISTINCT foi.order_item_key) as times_sold
FROM dim_products dp
LEFT JOIN fact_order_items foi ON dp.product_key = foi.product_key
GROUP BY dp.product_key, dp.name, dp.category, dp.price_tier, dp.profit_margin
ORDER BY times_sold DESC;

-- Revenue by day
SELECT 
    dd.full_date,
    dd.day_name,
    fds.total_orders,
    fds.total_revenue,
    fds.avg_order_value
FROM fact_daily_sales fds
JOIN dim_date dd ON fds.date_key = dd.date_key
ORDER BY dd.full_date;

-- Sales by customer segment
SELECT 
    dc.customer_segment,
    COUNT(fo.order_key) as orders,
    SUM(fo.total_amount) as revenue,
    AVG(fo.total_amount) as avg_order_value
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_key = dc.customer_key
GROUP BY dc.customer_segment
ORDER BY revenue DESC;

\q
Summary: What You've Learned
Component	What You Did	Skills Demonstrated
PostgreSQL	Created source/warehouse DBs, loaded data	SQL, Data Modeling
ETL Pipeline	Built Extract-Transform-Load process	Python, Pandas, ETL
Star Schema	Created dimensions and fact tables	Data Warehousing
Kafka	Produced/consumed real-time events	Stream Processing
MinIO	Uploaded data to object storage	Data Lake, S3
Airflow	Orchestrated pipeline workflows	Workflow Orchestration
Grafana	Created monitoring dashboards	Observability
Metabase	Built BI reports	Business Intelligence

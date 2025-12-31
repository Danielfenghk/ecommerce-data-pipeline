#!/usr/bin/env python3
"""
Manual ETL Runner - Fixed Version
Works with your actual database schema
"""

import os
import sys
import warnings
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Suppress pandas warning about psycopg2
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

# Detect environment and set connection strings
def is_running_in_docker():
    """Check if running inside Docker container."""
    return os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER')

if is_running_in_docker():
    print("🐳 Running inside Docker")
    SOURCE_DB = {
        'host': 'postgres-source',
        'port': 5432,
        'database': 'ecommerce_source',
        'user': 'ecommerce_user',
        'password': 'ecommerce_pass'
    }
    WAREHOUSE_DB = {
        'host': 'postgres-warehouse',
        'port': 5432,
        'database': 'ecommerce_warehouse',
        'user': 'warehouse_user',
        'password': 'warehouse_pass'
    }
else:
    print("💻 Running on local machine (Windows/WSL)")
    SOURCE_DB = {
        'host': 'localhost',
        'port': 5432,
        'database': 'ecommerce_source',
        'user': 'ecommerce_user',
        'password': 'ecommerce_pass'
    }
    WAREHOUSE_DB = {
        'host': 'localhost',
        'port': 5433,  # Different port for warehouse!
        'database': 'ecommerce_warehouse',
        'user': 'warehouse_user',
        'password': 'warehouse_pass'
    }


# =============================================================================
# DATABASE UTILITIES
# =============================================================================

def get_connection(db_config):
    """Create database connection."""
    return psycopg2.connect(**db_config)


def get_table_columns(conn, table_name):
    """Get list of columns for a table."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = [row[0] for row in cur.fetchall()]
    cur.close()
    return columns


def table_exists(conn, table_name):
    """Check if table exists."""
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
    """, (table_name,))
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def fetch_dataframe(conn, query):
    """Fetch query results as DataFrame (avoiding pandas warning)."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    
    if rows:
        return pd.DataFrame(rows)
    else:
        return pd.DataFrame()


# =============================================================================
# ENSURE SOURCE DATA EXISTS
# =============================================================================

def ensure_source_data():
    """Ensure source tables exist and have data."""
    print("\n🔧 Checking source database...")
    
    conn = get_connection(SOURCE_DB)
    cur = conn.cursor()
    
    # Check if customers table exists
    if not table_exists(conn, 'customers'):
        print("   Creating source tables...")
        
        # Create customers
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                city VARCHAR(100),
                state VARCHAR(50),
                country VARCHAR(100),
                postal_code VARCHAR(20),
                registration_date TIMESTAMP,
                customer_segment VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create products
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255),
                category VARCHAR(100),
                subcategory VARCHAR(100),
                brand VARCHAR(100),
                price DECIMAL(10,2),
                cost DECIMAL(10,2),
                stock_quantity INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create orders
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                order_date TIMESTAMP,
                status VARCHAR(50),
                payment_method VARCHAR(50),
                subtotal DECIMAL(12,2),
                tax DECIMAL(10,2),
                shipping_cost DECIMAL(10,2),
                total_amount DECIMAL(12,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create order_items
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                item_id SERIAL PRIMARY KEY,
                order_id VARCHAR(50),
                product_id VARCHAR(50),
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                discount DECIMAL(10,2) DEFAULT 0,
                total_price DECIMAL(12,2)
            )
        """)
        
        conn.commit()
        print("   ✅ Source tables created")
    
    # Check if data exists
    cur.execute("SELECT COUNT(*) FROM customers")
    customer_count = cur.fetchone()[0]
    
    if customer_count == 0:
        print("   Loading sample data...")
        
        # Insert customers
        cur.execute("""
            INSERT INTO customers (customer_id, first_name, last_name, email, city, state, country, postal_code, registration_date, customer_segment) VALUES
            ('CUST001', 'John', 'Smith', 'john.smith@email.com', 'New York', 'NY', 'USA', '10001', '2023-06-15', 'Premium'),
            ('CUST002', 'Emily', 'Johnson', 'emily.j@email.com', 'Los Angeles', 'CA', 'USA', '90001', '2023-07-20', 'Regular'),
            ('CUST003', 'Michael', 'Williams', 'm.williams@email.com', 'Chicago', 'IL', 'USA', '60601', '2023-08-10', 'Premium'),
            ('CUST004', 'Sarah', 'Brown', 'sarah.brown@email.com', 'Houston', 'TX', 'USA', '77001', '2023-09-05', 'Regular'),
            ('CUST005', 'David', 'Jones', 'david.jones@email.com', 'Phoenix', 'AZ', 'USA', '85001', '2023-10-12', 'New')
            ON CONFLICT (customer_id) DO NOTHING
        """)
        
        # Insert products
        cur.execute("""
            INSERT INTO products (product_id, name, category, subcategory, brand, price, cost, stock_quantity) VALUES
            ('PROD001', 'Wireless Bluetooth Headphones', 'Electronics', 'Audio', 'SoundMax', 79.99, 45.00, 150),
            ('PROD002', 'Organic Green Tea', 'Food & Beverages', 'Tea', 'NatureBrew', 12.99, 6.50, 500),
            ('PROD003', 'Running Shoes Pro', 'Sports & Outdoors', 'Footwear', 'SpeedRunner', 129.99, 75.00, 200),
            ('PROD004', 'Stainless Steel Water Bottle', 'Home & Kitchen', 'Drinkware', 'EcoLife', 24.99, 12.00, 300),
            ('PROD005', 'Smart Watch Series 5', 'Electronics', 'Wearables', 'TechTime', 299.99, 180.00, 100)
            ON CONFLICT (product_id) DO NOTHING
        """)
        
        # Insert orders
        cur.execute("""
            INSERT INTO orders (order_id, customer_id, order_date, status, payment_method, subtotal, tax, shipping_cost, total_amount) VALUES
            ('ORD-001', 'CUST001', '2024-01-15 10:30:00', 'completed', 'credit_card', 129.97, 10.40, 5.99, 146.36),
            ('ORD-002', 'CUST003', '2024-01-16 14:45:00', 'completed', 'paypal', 269.99, 21.60, 0, 291.59),
            ('ORD-003', 'CUST002', '2024-01-17 09:15:00', 'shipped', 'credit_card', 169.98, 13.60, 7.99, 191.57),
            ('ORD-004', 'CUST005', '2024-01-18 16:00:00', 'processing', 'debit_card', 76.95, 6.16, 5.99, 89.10),
            ('ORD-005', 'CUST004', '2024-01-19 11:30:00', 'completed', 'credit_card', 109.97, 8.80, 0, 118.77)
            ON CONFLICT (order_id) DO NOTHING
        """)
        
        # Insert order items
        cur.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount, total_price) VALUES
            ('ORD-001', 'PROD001', 1, 79.99, 0, 79.99),
            ('ORD-001', 'PROD004', 2, 24.99, 0, 49.98),
            ('ORD-002', 'PROD005', 1, 299.99, 30.00, 269.99),
            ('ORD-003', 'PROD003', 1, 129.99, 0, 129.99),
            ('ORD-003', 'PROD004', 1, 24.99, 0, 24.99),
            ('ORD-004', 'PROD002', 3, 12.99, 0, 38.97),
            ('ORD-005', 'PROD001', 1, 79.99, 0, 79.99)
        """)
        
        conn.commit()
        print("   ✅ Sample data loaded")
    else:
        print(f"   ✅ Source data exists ({customer_count} customers)")
    
    # Show actual table structure
    print("\n   📋 Source table columns:")
    for table in ['customers', 'products', 'orders', 'order_items']:
        cols = get_table_columns(conn, table)
        print(f"      {table}: {', '.join(cols[:5])}{'...' if len(cols) > 5 else ''}")
    
    cur.close()
    conn.close()


# =============================================================================
# ENSURE WAREHOUSE TABLES
# =============================================================================

def ensure_warehouse_tables():
    """Create warehouse tables if they don't exist."""
    print("\n🔧 Checking warehouse database...")
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    # Create dim_date
    cur.execute("""
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
        )
    """)
    
    # Create dim_customers
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_key SERIAL PRIMARY KEY,
            customer_id VARCHAR(50) UNIQUE NOT NULL,
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
        )
    """)
    
    # Create dim_products
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_products (
            product_key SERIAL PRIMARY KEY,
            product_id VARCHAR(50) UNIQUE NOT NULL,
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
        )
    """)
    
    # Create fact_orders
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_key SERIAL PRIMARY KEY,
            order_id VARCHAR(50) UNIQUE NOT NULL,
            customer_key INTEGER,
            date_key INTEGER,
            order_status VARCHAR(50),
            payment_method VARCHAR(50),
            subtotal DECIMAL(12,2),
            tax_amount DECIMAL(10,2),
            shipping_amount DECIMAL(10,2),
            discount_amount DECIMAL(10,2) DEFAULT 0,
            total_amount DECIMAL(12,2),
            item_count INTEGER,
            order_value_tier VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create fact_daily_sales
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_daily_sales (
            daily_sales_key SERIAL PRIMARY KEY,
            date_key INTEGER,
            total_orders INTEGER,
            total_revenue DECIMAL(14,2),
            avg_order_value DECIMAL(10,2),
            unique_customers INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date_key)
        )
    """)
    
    # Create etl_job_log (FIXED - simpler schema)
    cur.execute("""
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
        )
    """)
    
    conn.commit()
    
    # Populate date dimension if empty
    cur.execute("SELECT COUNT(*) FROM dim_date")
    if cur.fetchone()[0] == 0:
        print("   Populating date dimension...")
        cur.execute("""
            INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month, 
                                  week_of_year, month_number, month_name, quarter, year, is_weekend)
            SELECT 
                TO_CHAR(d, 'YYYYMMDD')::INTEGER,
                d,
                EXTRACT(DOW FROM d)::INTEGER,
                TRIM(TO_CHAR(d, 'Day')),
                EXTRACT(DAY FROM d)::INTEGER,
                EXTRACT(WEEK FROM d)::INTEGER,
                EXTRACT(MONTH FROM d)::INTEGER,
                TRIM(TO_CHAR(d, 'Month')),
                EXTRACT(QUARTER FROM d)::INTEGER,
                EXTRACT(YEAR FROM d)::INTEGER,
                EXTRACT(DOW FROM d) IN (0, 6)
            FROM generate_series('2023-01-01'::date, '2025-12-31'::date, '1 day'::interval) d
            ON CONFLICT (date_key) DO NOTHING
        """)
        conn.commit()
        print("   ✅ Date dimension populated")
    
    print("   ✅ Warehouse tables ready")
    
    cur.close()
    conn.close()


# =============================================================================
# EXTRACT FUNCTIONS
# =============================================================================

def extract_customers():
    """Extract customers from source database."""
    print("📥 Extracting customers...")
    conn = get_connection(SOURCE_DB)
    df = fetch_dataframe(conn, "SELECT * FROM customers")
    conn.close()
    print(f"   ✅ Extracted {len(df)} customers")
    return df


def extract_products():
    """Extract products from source database."""
    print("📥 Extracting products...")
    conn = get_connection(SOURCE_DB)
    df = fetch_dataframe(conn, "SELECT * FROM products")
    conn.close()
    print(f"   ✅ Extracted {len(df)} products")
    return df


def extract_orders():
    """Extract orders from source database - FIXED query."""
    print("📥 Extracting orders...")
    conn = get_connection(SOURCE_DB)
    
    # First, get actual columns in orders table
    order_columns = get_table_columns(conn, 'orders')
    print(f"   Available columns: {order_columns}")
    
    # Build dynamic query based on actual columns
    df = fetch_dataframe(conn, """
        SELECT 
            o.order_id,
            o.customer_id,
            o.order_date,
            o.status,
            o.payment_method,
            o.subtotal,
            o.tax,
            o.shipping_cost,
            o.total_amount,
            COUNT(oi.item_id) as item_count,
            COALESCE(SUM(oi.discount), 0) as total_discount
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.customer_id, o.order_date, o.status,
                 o.payment_method, o.subtotal, o.tax, o.shipping_cost, o.total_amount
    """)
    conn.close()
    print(f"   ✅ Extracted {len(df)} orders")
    return df


def extract_order_items():
    """Extract order items from source database."""
    print("📥 Extracting order items...")
    conn = get_connection(SOURCE_DB)
    df = fetch_dataframe(conn, "SELECT * FROM order_items")
    conn.close()
    print(f"   ✅ Extracted {len(df)} order items")
    return df


# =============================================================================
# TRANSFORM FUNCTIONS
# =============================================================================

def transform_customers(df):
    """Transform customers for warehouse."""
    print("🔄 Transforming customers...")
    
    if df.empty:
        print("   ⚠️  No customers to transform")
        return df
    
    result = df.copy()
    
    # Create full name
    result['full_name'] = result['first_name'].fillna('') + ' ' + result['last_name'].fillna('')
    result['full_name'] = result['full_name'].str.strip()
    
    # Calculate customer tenure (days since registration)
    if 'registration_date' in result.columns:
        result['registration_date'] = pd.to_datetime(result['registration_date'], errors='coerce')
        result['customer_tenure_days'] = (
            pd.Timestamp.now() - result['registration_date']
        ).dt.days.fillna(0).astype(int)
    else:
        result['customer_tenure_days'] = 0
    
    # Add SCD metadata
    result['effective_date'] = datetime.now().date()
    result['is_current'] = True
    
    print(f"   ✅ Transformed {len(result)} customers")
    return result


def transform_products(df):
    """Transform products for warehouse."""
    print("🔄 Transforming products...")
    
    if df.empty:
        print("   ⚠️  No products to transform")
        return df
    
    result = df.copy()
    
    # Calculate profit margin
    if 'price' in result.columns and 'cost' in result.columns:
        result['current_price'] = result['price']
        result['profit_margin'] = (
            (result['price'] - result['cost']) / result['price'] * 100
        ).round(2).fillna(0)
    else:
        result['current_price'] = 0
        result['profit_margin'] = 0
    
    # Create price tier
    def get_price_tier(price):
        if pd.isna(price) or price == 0:
            return 'Unknown'
        elif price < 20:
            return 'Budget'
        elif price < 50:
            return 'Standard'
        elif price < 100:
            return 'Premium'
        else:
            return 'Luxury'
    
    result['price_tier'] = result['price'].apply(get_price_tier)
    
    # Add SCD metadata
    result['effective_date'] = datetime.now().date()
    result['is_current'] = True
    
    print(f"   ✅ Transformed {len(result)} products")
    return result


def transform_orders(df):
    """Transform orders for warehouse."""
    print("🔄 Transforming orders...")
    
    if df.empty:
        print("   ⚠️  No orders to transform")
        return df
    
    result = df.copy()
    
    # Create date key from order_date
    result['order_date'] = pd.to_datetime(result['order_date'], errors='coerce')
    result['date_key'] = result['order_date'].dt.strftime('%Y%m%d').astype(int)
    
    # Create order value tier
    def get_value_tier(amount):
        if pd.isna(amount):
            return 'Unknown'
        elif amount < 50:
            return 'Very Low'
        elif amount < 100:
            return 'Low'
        elif amount < 200:
            return 'Medium'
        elif amount < 500:
            return 'High'
        else:
            return 'Very High'
    
    result['order_value_tier'] = result['total_amount'].apply(get_value_tier)
    result['discount_amount'] = result.get('total_discount', pd.Series([0]*len(result))).fillna(0)
    
    print(f"   ✅ Transformed {len(result)} orders")
    return result


# =============================================================================
# LOAD FUNCTIONS
# =============================================================================

def load_dim_customers(df):
    """Load customers into dimension table."""
    print("📤 Loading dim_customers...")
    
    if df.empty:
        print("   ⚠️  No customers to load")
        return
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    loaded = 0
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO dim_customers 
                    (customer_id, first_name, last_name, full_name, email, 
                     city, state, country, customer_segment, registration_date,
                     customer_tenure_days, is_current, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    full_name = EXCLUDED.full_name,
                    customer_tenure_days = EXCLUDED.customer_tenure_days,
                    effective_date = EXCLUDED.effective_date
            """, (
                row['customer_id'],
                row.get('first_name'),
                row.get('last_name'),
                row.get('full_name'),
                row.get('email'),
                row.get('city'),
                row.get('state'),
                row.get('country'),
                row.get('customer_segment'),
                row.get('registration_date'),
                int(row.get('customer_tenure_days', 0)),
                True,
                datetime.now().date()
            ))
            loaded += 1
        except Exception as e:
            print(f"   ⚠️  Error loading customer {row['customer_id']}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   ✅ Loaded {loaded} customers")


def load_dim_products(df):
    """Load products into dimension table."""
    print("📤 Loading dim_products...")
    
    if df.empty:
        print("   ⚠️  No products to load")
        return
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    loaded = 0
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO dim_products 
                    (product_id, name, category, subcategory, brand,
                     current_price, cost, profit_margin, price_tier,
                     is_current, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    current_price = EXCLUDED.current_price,
                    profit_margin = EXCLUDED.profit_margin,
                    price_tier = EXCLUDED.price_tier,
                    effective_date = EXCLUDED.effective_date
            """, (
                row['product_id'],
                row.get('name'),
                row.get('category'),
                row.get('subcategory'),
                row.get('brand'),
                float(row.get('current_price', 0)),
                float(row.get('cost', 0)),
                float(row.get('profit_margin', 0)),
                row.get('price_tier'),
                True,
                datetime.now().date()
            ))
            loaded += 1
        except Exception as e:
            print(f"   ⚠️  Error loading product {row['product_id']}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   ✅ Loaded {loaded} products")


def load_fact_orders(df):
    """Load orders into fact table."""
    print("📤 Loading fact_orders...")
    
    if df.empty:
        print("   ⚠️  No orders to load")
        return
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    # Get customer key mapping
    cur.execute("SELECT customer_key, customer_id FROM dim_customers")
    customer_map = {row[1]: row[0] for row in cur.fetchall()}
    
    loaded = 0
    for _, row in df.iterrows():
        try:
            customer_key = customer_map.get(row['customer_id'])
            
            cur.execute("""
                INSERT INTO fact_orders 
                    (order_id, customer_key, date_key, order_status, payment_method,
                     subtotal, tax_amount, shipping_amount, discount_amount,
                     total_amount, item_count, order_value_tier)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    order_status = EXCLUDED.order_status,
                    total_amount = EXCLUDED.total_amount,
                    item_count = EXCLUDED.item_count
            """, (
                row['order_id'],
                customer_key,
                int(row['date_key']),
                row.get('status'),
                row.get('payment_method'),
                float(row.get('subtotal', 0)),
                float(row.get('tax', 0)),
                float(row.get('shipping_cost', 0)),
                float(row.get('discount_amount', 0)),
                float(row.get('total_amount', 0)),
                int(row.get('item_count', 0)),
                row.get('order_value_tier')
            ))
            loaded += 1
        except Exception as e:
            print(f"   ⚠️  Error loading order {row['order_id']}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   ✅ Loaded {loaded} orders")


def calculate_daily_aggregates():
    """Calculate and load daily sales aggregates."""
    print("📊 Calculating daily aggregates...")
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    # Delete existing aggregates and recalculate
    cur.execute("DELETE FROM fact_daily_sales")
    
    cur.execute("""
        INSERT INTO fact_daily_sales 
            (date_key, total_orders, total_revenue, avg_order_value, unique_customers)
        SELECT 
            date_key,
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_key) as unique_customers
        FROM fact_orders
        WHERE date_key IS NOT NULL
        GROUP BY date_key
    """)
    
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM fact_daily_sales")
    count = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    print(f"   ✅ Created {count} daily aggregate records")


def log_etl_job(job_name, status, records_processed, start_time, error=None):
    """Log ETL job execution - FIXED version."""
    try:
        conn = get_connection(WAREHOUSE_DB)
        cur = conn.cursor()
        
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())
        
        # FIXED: Removed job_type column
        cur.execute("""
            INSERT INTO etl_job_log 
                (job_name, status, records_processed, start_time, end_time, 
                 duration_seconds, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            job_name,
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
    except Exception as e:
        print(f"   ⚠️  Warning: Could not log ETL job: {e}")


# =============================================================================
# VERIFICATION
# =============================================================================

def verify_results():
    """Verify and display ETL results."""
    print("\n📋 VERIFICATION")
    print("=" * 60)
    
    conn = get_connection(WAREHOUSE_DB)
    cur = conn.cursor()
    
    # Count records in each table
    tables = [
        ('dim_date', 'date_key'),
        ('dim_customers', 'customer_key'),
        ('dim_products', 'product_key'),
        ('fact_orders', 'order_key'),
        ('fact_daily_sales', 'daily_sales_key'),
    ]
    
    print("\n   📊 Warehouse Table Counts:")
    for table, pk in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"      {table}: {count} rows")
        except Exception as e:
            print(f"      {table}: Error - {e}")
    
    # Sample data from fact_orders
    print("\n   📦 Sample Orders (Top 5):")
    try:
        cur.execute("""
            SELECT 
                fo.order_id, 
                dc.full_name as customer,
                fo.total_amount,
                fo.order_status,
                fo.order_value_tier
            FROM fact_orders fo
            LEFT JOIN dim_customers dc ON fo.customer_key = dc.customer_key
            ORDER BY fo.total_amount DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        for row in rows:
            print(f"      {row[0]} | {row[1]} | ${row[2]:.2f} | {row[3]} | {row[4]}")
    except Exception as e:
        print(f"      Error: {e}")
    
    # Daily sales summary
    print("\n   📈 Daily Sales Summary:")
    try:
        cur.execute("""
            SELECT 
                dd.full_date,
                fds.total_orders,
                fds.total_revenue,
                fds.avg_order_value
            FROM fact_daily_sales fds
            JOIN dim_date dd ON fds.date_key = dd.date_key
            ORDER BY dd.full_date
            LIMIT 5
        """)
        rows = cur.fetchall()
        for row in rows:
            print(f"      {row[0]} | Orders: {row[1]} | Revenue: ${row[2]:.2f} | Avg: ${row[3]:.2f}")
    except Exception as e:
        print(f"      Error: {e}")
    
    cur.close()
    conn.close()


# =============================================================================
# MAIN ETL PIPELINE
# =============================================================================

def run_full_etl():
    """Run the complete ETL pipeline."""
    
    print("\n" + "=" * 60)
    print("🚀 E-COMMERCE ETL PIPELINE")
    print("=" * 60)
    
    start_time = datetime.now()
    total_records = 0
    
    try:
        # Step 0: Ensure databases are ready
        ensure_source_data()
        ensure_warehouse_tables()
        
        # Step 1: EXTRACT
        print("\n" + "=" * 60)
        print("📥 EXTRACT PHASE")
        print("=" * 60)
        
        customers_df = extract_customers()
        products_df = extract_products()
        orders_df = extract_orders()
        
        total_records = len(customers_df) + len(products_df) + len(orders_df)
        
        # Step 2: TRANSFORM
        print("\n" + "=" * 60)
        print("🔄 TRANSFORM PHASE")
        print("=" * 60)
        
        customers_transformed = transform_customers(customers_df)
        products_transformed = transform_products(products_df)
        orders_transformed = transform_orders(orders_df)
        
        # Step 3: LOAD
        print("\n" + "=" * 60)
        print("📤 LOAD PHASE")
        print("=" * 60)
        
        load_dim_customers(customers_transformed)
        load_dim_products(products_transformed)
        load_fact_orders(orders_transformed)
        
        # Step 4: AGGREGATE
        print("\n" + "=" * 60)
        print("📊 AGGREGATION PHASE")
        print("=" * 60)
        
        calculate_daily_aggregates()
        
        # Step 5: LOG SUCCESS
        log_etl_job('full_etl_pipeline', 'SUCCESS', total_records, start_time)
        
        # Step 6: VERIFY
        verify_results()
        
        # Final summary
        duration = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"   📊 Total records processed: {total_records}")
        print(f"   ⏱️  Duration: {duration:.2f} seconds")
        print(f"   🕐 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60 + "\n")
        
        return True
        
    except Exception as e:
        # Log failure
        log_etl_job('full_etl_pipeline', 'FAILED', total_records, start_time, str(e))
        
        print("\n" + "=" * 60)
        print("❌ ETL PIPELINE FAILED!")
        print("=" * 60)
        print(f"   Error: {e}")
        print("=" * 60 + "\n")
        
        import traceback
        traceback.print_exc()
        
        return False


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    success = run_full_etl()
    sys.exit(0 if success else 1)
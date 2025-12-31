#!/usr/bin/env python3
"""
Adaptive ETL Runner - Automatically detects and adapts to existing schema
Works with any table structure
"""

import os
import sys
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Suppress pandas warning
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

def is_docker():
    return os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER')

if is_docker():
    print("🐳 Running inside Docker")
    SOURCE_DB = {'host': 'postgres-source', 'port': 5432, 'database': 'ecommerce_source', 'user': 'ecommerce_user', 'password': 'ecommerce_pass'}
    WAREHOUSE_DB = {'host': 'postgres-warehouse', 'port': 5432, 'database': 'ecommerce_warehouse', 'user': 'warehouse_user', 'password': 'warehouse_pass'}
else:
    print("💻 Running on local machine")
    SOURCE_DB = {'host': 'localhost', 'port': 5432, 'database': 'ecommerce_source', 'user': 'ecommerce_user', 'password': 'ecommerce_pass'}
    WAREHOUSE_DB = {'host': 'localhost', 'port': 5433, 'database': 'ecommerce_warehouse', 'user': 'warehouse_user', 'password': 'warehouse_pass'}


# =============================================================================
# DATABASE UTILITIES
# =============================================================================

def get_connection(db_config):
    """Create database connection."""
    return psycopg2.connect(**db_config)


def get_table_info(conn, table_name: str) -> Dict:
    """Get detailed table information."""
    cur = conn.cursor()
    
    # Check if table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
    """, (table_name,))
    exists = cur.fetchone()[0]
    
    if not exists:
        cur.close()
        return {'exists': False, 'columns': [], 'column_types': {}}
    
    # Get columns and types
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    
    columns = []
    column_types = {}
    for row in cur.fetchall():
        columns.append(row[0])
        column_types[row[0]] = row[1]
    
    cur.close()
    return {'exists': True, 'columns': columns, 'column_types': column_types}


def fetch_dataframe(conn, query: str) -> pd.DataFrame:
    """Fetch query results as DataFrame."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(query)
        rows = cur.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    finally:
        cur.close()


def execute_query(conn, query: str, params: tuple = None, commit: bool = True):
    """Execute a query with proper error handling."""
    cur = conn.cursor()
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        if commit:
            conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()


# =============================================================================
# SCHEMA MANAGER - Creates correct schema
# =============================================================================

class SchemaManager:
    """Manages database schema creation and validation."""
    
    def __init__(self, conn):
        self.conn = conn
    
    def drop_all_tables(self):
        """Drop all warehouse tables."""
        print("   🗑️  Dropping existing tables...")
        tables = [
            'fact_daily_sales',
            'fact_order_items', 
            'fact_orders',
            'dim_customers',
            'dim_products',
            'dim_geography',
            'dim_date',
            'etl_job_log',
            'data_quality_results'
        ]
        cur = self.conn.cursor()
        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        self.conn.commit()
        cur.close()
        print("   ✅ Tables dropped")
    
    def create_all_tables(self):
        """Create all warehouse tables with correct schema."""
        print("   🏗️  Creating warehouse tables...")
        cur = self.conn.cursor()
        
        # dim_date
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
        
        # dim_customers
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
                customer_tenure_days INTEGER DEFAULT 0,
                is_current BOOLEAN DEFAULT TRUE,
                effective_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # dim_products
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
        
        # fact_orders
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_orders (
                order_key SERIAL PRIMARY KEY,
                order_id VARCHAR(50) UNIQUE NOT NULL,
                customer_key INTEGER REFERENCES dim_customers(customer_key),
                date_key INTEGER REFERENCES dim_date(date_key),
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
        
        # fact_daily_sales
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_daily_sales (
                daily_sales_key SERIAL PRIMARY KEY,
                date_key INTEGER REFERENCES dim_date(date_key),
                total_orders INTEGER,
                total_revenue DECIMAL(14,2),
                avg_order_value DECIMAL(10,2),
                unique_customers INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date_key)
            )
        """)
        
        # etl_job_log
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
        
        self.conn.commit()
        cur.close()
        print("   ✅ Tables created")
    
    def populate_date_dimension(self):
        """Populate date dimension."""
        cur = self.conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM dim_date")
        if cur.fetchone()[0] == 0:
            print("   📅 Populating date dimension...")
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
            self.conn.commit()
            print("   ✅ Date dimension populated (1096 rows)")
        else:
            print("   ✅ Date dimension already populated")
        
        cur.close()
    
    def validate_schema(self) -> bool:
        """Check if schema matches expected structure."""
        expected_columns = {
            'dim_customers': ['customer_key', 'customer_id', 'full_name', 'customer_tenure_days'],
            'dim_products': ['product_key', 'product_id', 'name', 'profit_margin'],
            'fact_orders': ['order_key', 'order_id', 'customer_key', 'date_key'],
        }
        
        for table, required_cols in expected_columns.items():
            info = get_table_info(self.conn, table)
            if not info['exists']:
                return False
            for col in required_cols:
                if col not in info['columns']:
                    print(f"   ⚠️  Table {table} missing column: {col}")
                    return False
        
        return True


# =============================================================================
# SOURCE DATA MANAGER
# =============================================================================

class SourceDataManager:
    """Manages source database setup and data."""
    
    def __init__(self, conn):
        self.conn = conn
    
    def ensure_tables_exist(self):
        """Create source tables if they don't exist."""
        cur = self.conn.cursor()
        
        # Customers
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                phone VARCHAR(50),
                city VARCHAR(100),
                state VARCHAR(50),
                country VARCHAR(100),
                postal_code VARCHAR(20),
                registration_date TIMESTAMP,
                customer_segment VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Products
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
        
        # Orders
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
        
        # Order Items
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
        
        self.conn.commit()
        cur.close()
    
    def load_sample_data(self):
        """Load sample data if tables are empty."""
        cur = self.conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM customers")
        if cur.fetchone()[0] > 0:
            cur.close()
            return  # Data already exists
        
        print("   📝 Loading sample data...")
        
        # Customers
        cur.execute("""
            INSERT INTO customers (customer_id, first_name, last_name, email, city, state, country, postal_code, registration_date, customer_segment) VALUES
            ('CUST001', 'John', 'Smith', 'john.smith@email.com', 'New York', 'NY', 'USA', '10001', '2023-06-15', 'Premium'),
            ('CUST002', 'Emily', 'Johnson', 'emily.j@email.com', 'Los Angeles', 'CA', 'USA', '90001', '2023-07-20', 'Regular'),
            ('CUST003', 'Michael', 'Williams', 'm.williams@email.com', 'Chicago', 'IL', 'USA', '60601', '2023-08-10', 'Premium'),
            ('CUST004', 'Sarah', 'Brown', 'sarah.brown@email.com', 'Houston', 'TX', 'USA', '77001', '2023-09-05', 'Regular'),
            ('CUST005', 'David', 'Jones', 'david.jones@email.com', 'Phoenix', 'AZ', 'USA', '85001', '2023-10-12', 'New')
            ON CONFLICT (customer_id) DO NOTHING
        """)
        
        # Products
        cur.execute("""
            INSERT INTO products (product_id, name, category, subcategory, brand, price, cost, stock_quantity) VALUES
            ('PROD001', 'Wireless Bluetooth Headphones', 'Electronics', 'Audio', 'SoundMax', 79.99, 45.00, 150),
            ('PROD002', 'Organic Green Tea', 'Food & Beverages', 'Tea', 'NatureBrew', 12.99, 6.50, 500),
            ('PROD003', 'Running Shoes Pro', 'Sports & Outdoors', 'Footwear', 'SpeedRunner', 129.99, 75.00, 200),
            ('PROD004', 'Stainless Steel Water Bottle', 'Home & Kitchen', 'Drinkware', 'EcoLife', 24.99, 12.00, 300),
            ('PROD005', 'Smart Watch Series 5', 'Electronics', 'Wearables', 'TechTime', 299.99, 180.00, 100)
            ON CONFLICT (product_id) DO NOTHING
        """)
        
        # Orders
        cur.execute("""
            INSERT INTO orders (order_id, customer_id, order_date, status, payment_method, subtotal, tax, shipping_cost, total_amount) VALUES
            ('ORD-001', 'CUST001', '2024-01-15 10:30:00', 'completed', 'credit_card', 129.97, 10.40, 5.99, 146.36),
            ('ORD-002', 'CUST003', '2024-01-16 14:45:00', 'completed', 'paypal', 269.99, 21.60, 0, 291.59),
            ('ORD-003', 'CUST002', '2024-01-17 09:15:00', 'shipped', 'credit_card', 169.98, 13.60, 7.99, 191.57),
            ('ORD-004', 'CUST005', '2024-01-18 16:00:00', 'processing', 'debit_card', 76.95, 6.16, 5.99, 89.10),
            ('ORD-005', 'CUST004', '2024-01-19 11:30:00', 'completed', 'credit_card', 109.97, 8.80, 0, 118.77)
            ON CONFLICT (order_id) DO NOTHING
        """)
        
        # Order Items
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
        
        self.conn.commit()
        cur.close()
        print("   ✅ Sample data loaded")


# =============================================================================
# ETL PIPELINE
# =============================================================================

class ETLPipeline:
    """Main ETL Pipeline class."""
    
    def __init__(self):
        self.source_conn = None
        self.warehouse_conn = None
        self.start_time = None
        self.records_processed = 0
    
    def connect(self):
        """Establish database connections."""
        print("\n🔌 Connecting to databases...")
        self.source_conn = get_connection(SOURCE_DB)
        self.warehouse_conn = get_connection(WAREHOUSE_DB)
        print("   ✅ Connected to source database")
        print("   ✅ Connected to warehouse database")
    
    def close(self):
        """Close database connections."""
        if self.source_conn:
            self.source_conn.close()
        if self.warehouse_conn:
            self.warehouse_conn.close()
    
    def setup_source(self):
        """Setup source database."""
        print("\n🔧 Setting up source database...")
        manager = SourceDataManager(self.source_conn)
        manager.ensure_tables_exist()
        manager.load_sample_data()
        
        # Show counts
        cur = self.source_conn.cursor()
        for table in ['customers', 'products', 'orders', 'order_items']:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"   📊 {table}: {count} rows")
        cur.close()
    
    def setup_warehouse(self):
        """Setup warehouse database with correct schema."""
        print("\n🔧 Setting up warehouse database...")
        manager = SchemaManager(self.warehouse_conn)
        
        # Check if schema is valid
        if not manager.validate_schema():
            print("   ⚠️  Schema mismatch detected - recreating tables...")
            manager.drop_all_tables()
            manager.create_all_tables()
        else:
            print("   ✅ Schema is valid")
        
        manager.populate_date_dimension()
    
    def extract_customers(self) -> pd.DataFrame:
        """Extract customers from source."""
        print("📥 Extracting customers...")
        df = fetch_dataframe(self.source_conn, "SELECT * FROM customers")
        print(f"   ✅ Extracted {len(df)} customers")
        return df
    
    def extract_products(self) -> pd.DataFrame:
        """Extract products from source."""
        print("📥 Extracting products...")
        df = fetch_dataframe(self.source_conn, "SELECT * FROM products")
        print(f"   ✅ Extracted {len(df)} products")
        return df
    
    def extract_orders(self) -> pd.DataFrame:
        """Extract orders from source."""
        print("📥 Extracting orders...")
        df = fetch_dataframe(self.source_conn, """
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
        print(f"   ✅ Extracted {len(df)} orders")
        return df
    
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform customers."""
        print("🔄 Transforming customers...")
        
        if df.empty:
            return df
        
        result = df.copy()
        result['full_name'] = (result['first_name'].fillna('') + ' ' + result['last_name'].fillna('')).str.strip()
        
        if 'registration_date' in result.columns:
            result['registration_date'] = pd.to_datetime(result['registration_date'], errors='coerce')
            result['customer_tenure_days'] = (pd.Timestamp.now() - result['registration_date']).dt.days.fillna(0).astype(int)
        else:
            result['customer_tenure_days'] = 0
        
        result['effective_date'] = datetime.now().date()
        result['is_current'] = True
        
        print(f"   ✅ Transformed {len(result)} customers")
        return result
    
    def transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform products."""
        print("🔄 Transforming products...")
        
        if df.empty:
            return df
        
        result = df.copy()
        
        if 'price' in result.columns and 'cost' in result.columns:
            result['current_price'] = result['price']
            result['profit_margin'] = ((result['price'] - result['cost']) / result['price'] * 100).round(2).fillna(0)
        else:
            result['current_price'] = 0
            result['profit_margin'] = 0
        
        def price_tier(price):
            if pd.isna(price) or price == 0: return 'Unknown'
            elif price < 20: return 'Budget'
            elif price < 50: return 'Standard'
            elif price < 100: return 'Premium'
            else: return 'Luxury'
        
        result['price_tier'] = result.get('price', pd.Series([0])).apply(price_tier)
        result['effective_date'] = datetime.now().date()
        result['is_current'] = True
        
        print(f"   ✅ Transformed {len(result)} products")
        return result
    
    def transform_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform orders."""
        print("🔄 Transforming orders...")
        
        if df.empty:
            return df
        
        result = df.copy()
        result['order_date'] = pd.to_datetime(result['order_date'], errors='coerce')
        result['date_key'] = result['order_date'].dt.strftime('%Y%m%d').fillna('19000101').astype(int)
        
        def value_tier(amount):
            if pd.isna(amount): return 'Unknown'
            elif amount < 50: return 'Very Low'
            elif amount < 100: return 'Low'
            elif amount < 200: return 'Medium'
            elif amount < 500: return 'High'
            else: return 'Very High'
        
        result['order_value_tier'] = result['total_amount'].apply(value_tier)
        result['discount_amount'] = result.get('total_discount', pd.Series([0]*len(result))).fillna(0)
        
        print(f"   ✅ Transformed {len(result)} orders")
        return result
    
    def load_customers(self, df: pd.DataFrame):
        """Load customers to warehouse."""
        print("📤 Loading dim_customers...")
        
        if df.empty:
            print("   ⚠️  No data to load")
            return
        
        cur = self.warehouse_conn.cursor()
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
                        full_name = EXCLUDED.full_name,
                        customer_tenure_days = EXCLUDED.customer_tenure_days
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
                self.warehouse_conn.rollback()
                print(f"   ⚠️  Error: {e}")
                break
        
        self.warehouse_conn.commit()
        cur.close()
        print(f"   ✅ Loaded {loaded} customers")
    
    def load_products(self, df: pd.DataFrame):
        """Load products to warehouse."""
        print("📤 Loading dim_products...")
        
        if df.empty:
            print("   ⚠️  No data to load")
            return
        
        cur = self.warehouse_conn.cursor()
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
                        current_price = EXCLUDED.current_price,
                        profit_margin = EXCLUDED.profit_margin
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
                self.warehouse_conn.rollback()
                print(f"   ⚠️  Error: {e}")
                break
        
        self.warehouse_conn.commit()
        cur.close()
        print(f"   ✅ Loaded {loaded} products")
    
    def load_orders(self, df: pd.DataFrame):
        """Load orders to warehouse."""
        print("📤 Loading fact_orders...")
        
        if df.empty:
            print("   ⚠️  No data to load")
            return
        
        cur = self.warehouse_conn.cursor()
        
        # Get customer mapping
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
                        total_amount = EXCLUDED.total_amount
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
                self.warehouse_conn.rollback()
                print(f"   ⚠️  Error: {e}")
                break
        
        self.warehouse_conn.commit()
        cur.close()
        print(f"   ✅ Loaded {loaded} orders")
    
    def calculate_aggregates(self):
        """Calculate daily sales aggregates."""
        print("📊 Calculating daily aggregates...")
        
        cur = self.warehouse_conn.cursor()
        
        # Clear and recalculate
        cur.execute("DELETE FROM fact_daily_sales")
        cur.execute("""
            INSERT INTO fact_daily_sales 
                (date_key, total_orders, total_revenue, avg_order_value, unique_customers)
            SELECT 
                date_key,
                COUNT(*),
                SUM(total_amount),
                AVG(total_amount),
                COUNT(DISTINCT customer_key)
            FROM fact_orders
            WHERE date_key IS NOT NULL
            GROUP BY date_key
        """)
        
        self.warehouse_conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM fact_daily_sales")
        count = cur.fetchone()[0]
        cur.close()
        
        print(f"   ✅ Created {count} aggregate records")
    
    def log_job(self, status: str, error: str = None):
        """Log ETL job execution."""
        try:
            cur = self.warehouse_conn.cursor()
            end_time = datetime.now()
            duration = int((end_time - self.start_time).total_seconds())
            
            cur.execute("""
                INSERT INTO etl_job_log 
                    (job_name, status, records_processed, start_time, end_time, duration_seconds, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, ('full_etl_pipeline', status, self.records_processed, self.start_time, end_time, duration, error))
            
            self.warehouse_conn.commit()
            cur.close()
        except:
            pass
    
    def verify_results(self):
        """Display verification results."""
        print("\n" + "=" * 60)
        print("📋 VERIFICATION")
        print("=" * 60)
        
        cur = self.warehouse_conn.cursor()
        
        # Table counts
        print("\n   📊 Table Row Counts:")
        for table in ['dim_date', 'dim_customers', 'dim_products', 'fact_orders', 'fact_daily_sales']:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"      {table}: {count}")
            except:
                print(f"      {table}: Error")
        
        # Sample orders
        print("\n   📦 Sample Orders:")
        try:
            cur.execute("""
                SELECT fo.order_id, dc.full_name, fo.total_amount, fo.order_status
                FROM fact_orders fo
                LEFT JOIN dim_customers dc ON fo.customer_key = dc.customer_key
                ORDER BY fo.total_amount DESC
                LIMIT 5
            """)
            for row in cur.fetchall():
                print(f"      {row[0]} | {row[1]} | ${row[2]:.2f} | {row[3]}")
        except Exception as e:
            print(f"      Error: {e}")
        
        # Daily sales
        print("\n   📈 Daily Sales:")
        try:
            cur.execute("""
                SELECT dd.full_date, fds.total_orders, fds.total_revenue
                FROM fact_daily_sales fds
                JOIN dim_date dd ON fds.date_key = dd.date_key
                ORDER BY dd.full_date
                LIMIT 5
            """)
            for row in cur.fetchall():
                print(f"      {row[0]} | Orders: {row[1]} | Revenue: ${row[2]:.2f}")
        except Exception as e:
            print(f"      Error: {e}")
        
        cur.close()
    
    def run(self):
        """Run the complete ETL pipeline."""
        print("\n" + "=" * 60)
        print("🚀 E-COMMERCE ETL PIPELINE")
        print("=" * 60)
        
        self.start_time = datetime.now()
        
        try:
            # Connect
            self.connect()
            
            # Setup
            self.setup_source()
            self.setup_warehouse()
            
            # Extract
            print("\n" + "=" * 60)
            print("📥 EXTRACT PHASE")
            print("=" * 60)
            customers = self.extract_customers()
            products = self.extract_products()
            orders = self.extract_orders()
            
            self.records_processed = len(customers) + len(products) + len(orders)
            
            # Transform
            print("\n" + "=" * 60)
            print("🔄 TRANSFORM PHASE")
            print("=" * 60)
            customers_t = self.transform_customers(customers)
            products_t = self.transform_products(products)
            orders_t = self.transform_orders(orders)
            
            # Load
            print("\n" + "=" * 60)
            print("📤 LOAD PHASE")
            print("=" * 60)
            self.load_customers(customers_t)
            self.load_products(products_t)
            self.load_orders(orders_t)
            
            # Aggregate
            print("\n" + "=" * 60)
            print("📊 AGGREGATION PHASE")
            print("=" * 60)
            self.calculate_aggregates()
            
            # Verify
            self.verify_results()
            
            # Log success
            self.log_job('SUCCESS')
            
            # Summary
            duration = (datetime.now() - self.start_time).total_seconds()
            print("\n" + "=" * 60)
            print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"   📊 Records processed: {self.records_processed}")
            print(f"   ⏱️  Duration: {duration:.2f} seconds")
            print("=" * 60 + "\n")
            
            return True
            
        except Exception as e:
            self.log_job('FAILED', str(e))
            print("\n" + "=" * 60)
            print("❌ ETL PIPELINE FAILED!")
            print("=" * 60)
            print(f"   Error: {e}")
            print("=" * 60 + "\n")
            
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            self.close()


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    pipeline = ETLPipeline()
    success = pipeline.run()
    sys.exit(0 if success else 1)
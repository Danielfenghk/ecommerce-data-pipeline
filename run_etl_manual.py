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
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
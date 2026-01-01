#!/usr/bin/env python3
"""
Complete Lakehouse ETL Pipeline with Iceberg
"""

from datetime import datetime

from iceberg_bronze_ingestion import run_bronze_ingestion
from iceberg_silver_transform import run_silver_transform
from iceberg_gold_aggregate import run_gold_aggregate


def run_full_lakehouse_pipeline():
    """執行完整的 Lakehouse ETL 管道"""
    
    print("\n" + "=" * 70)
    print("E-COMMERCE DATA LAKEHOUSE PIPELINE")
    print("    Using Apache Iceberg + MinIO + Spark")
    print("=" * 70)
    
    start_time = datetime.now()
    
    try:
        # Step 1: Bronze Layer (Raw Data)
        run_bronze_ingestion()
        
        # Step 2: Silver Layer (Cleaned Data)
        run_silver_transform()
        
        # Step 3: Gold Layer (Aggregated Data)
        run_gold_aggregate()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print("✅ LAKEHOUSE PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print(f"   ⏱️  Duration: {duration:.2f} seconds")
        print("\n   Layers Created:")
        print("      Bronze: raw_customers, raw_products, raw_orders")
        print("      Silver: cleaned_customers, cleaned_products, cleaned_orders")
        print("      Gold:   dim_customers, dim_products, fact_orders,")
        print("              daily_sales_summary, customer_rfm")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_full_lakehouse_pipeline()
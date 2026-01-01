"""
Silver Layer
============

Data cleaning and standardization layer - transforms raw bronze data
into clean, consistent, and well-typed datasets.
"""

from typing import Optional, List, Dict, Callable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, lower, upper, coalesce, lit, when,
    current_timestamp, datediff, current_date, to_date,
    regexp_replace, concat_ws, round as spark_round,
    year, month, dayofmonth, date_format, to_timestamp
)
from pyspark.sql.types import DecimalType, StringType, IntegerType, DateType

from .iceberg_manager import IcebergManager


class SilverLayer:
    """
    Silver Layer: Data Cleaning and Standardization
    
    Responsibilities:
    - Clean and standardize data from Bronze layer
    - Apply data type conversions
    - Handle null values
    - Create derived columns
    - Apply business rules for data quality
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "nessie"):
        """
        Initialize Silver Layer.
        
        Args:
            spark: SparkSession with Iceberg configured
            catalog: Iceberg catalog name
        """
        self.spark = spark
        self.catalog = catalog
        self.bronze_db = "bronze"
        self.silver_db = "silver"
        self.manager = IcebergManager(spark, catalog)
        
        # Ensure database exists
        self.manager.create_database(self.silver_db)
    
    def _read_bronze(self, table_name: str) -> DataFrame:
        """Read table from Bronze layer."""
        return self.manager.read_table(self.bronze_db, table_name)
    
    def _write_silver(self, 
                      df: DataFrame, 
                      table_name: str,
                      partition_by: Optional[List[str]] = None,
                      merge_key: Optional[str] = None) -> int:
        """
        Write DataFrame to Silver layer.
        
        Args:
            df: Transformed DataFrame
            table_name: Target table name
            partition_by: Partitioning columns
            merge_key: Key for merge/upsert operation
        
        Returns:
            Number of rows written
        """
        # Add processing timestamp
        df = df.withColumn("_processed_at", current_timestamp())
        
        full_table_name = f"{self.catalog}.{self.silver_db}.{table_name}"
        
        if merge_key and self.manager.table_exists(self.silver_db, table_name):
            # Use MERGE for incremental updates
            df.createOrReplaceTempView("updates")
            
            self.spark.sql(f"""
                MERGE INTO {full_table_name} t
                USING updates s
                ON t.{merge_key} = s.{merge_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            count = df.count()
        else:
            # Create or replace table
            writer = df.writeTo(full_table_name) \
                .tableProperty("format-version", "2")
            
            if partition_by:
                writer = writer.partitionedBy(*partition_by)
            
            writer.createOrReplace()
            count = df.count()
        
        print(f"   ✅ Wrote {count} rows to {full_table_name}")
        return count
    
    def transform_customers(self) -> int:
        """
        Transform customers data.
        
        Transformations:
        - Trim and standardize names
        - Lowercase emails
        - Calculate customer tenure
        - Handle null segments
        """
        print("🔄 Transforming customers...")
        
        raw_df = self._read_bronze("raw_customers")
        
        cleaned = raw_df \
            .withColumn("first_name", trim(col("first_name"))) \
            .withColumn("last_name", trim(col("last_name"))) \
            .withColumn("full_name", 
                       concat_ws(" ", 
                                trim(col("first_name")), 
                                trim(col("last_name")))) \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("phone", 
                       regexp_replace(col("phone"), "[^0-9+\\-]", "")) \
            .withColumn("city", trim(col("city"))) \
            .withColumn("state", upper(trim(col("state")))) \
            .withColumn("country", trim(col("country"))) \
            .withColumn("postal_code", trim(col("postal_code"))) \
            .withColumn("customer_segment",
                       coalesce(col("customer_segment"), lit("Unknown"))) \
            .withColumn("registration_date",
                       to_date(col("registration_date"))) \
            .withColumn("tenure_days",
                       datediff(current_date(), to_date(col("registration_date")))) \
            .withColumn("is_active", lit(True)) \
            .select(
                "customer_id",
                "first_name",
                "last_name",
                "full_name",
                "email",
                "phone",
                "city",
                "state",
                "country",
                "postal_code",
                "customer_segment",
                "registration_date",
                "tenure_days",
                "is_active"
            )
        
        return self._write_silver(cleaned, "clean_customers", merge_key="customer_id")
    
    def transform_products(self) -> int:
        """
        Transform products data.
        
        Transformations:
        - Calculate profit margin
        - Create price tiers
        - Standardize categories
        - Add stock status
        """
        print("🔄 Transforming products...")
        
        raw_df = self._read_bronze("raw_products")
        
        cleaned = raw_df \
            .withColumn("product_name", trim(col("name"))) \
            .withColumn("category", trim(col("category"))) \
            .withColumn("subcategory", trim(col("subcategory"))) \
            .withColumn("brand", trim(col("brand"))) \
            .withColumn("price", col("price").cast(DecimalType(10, 2))) \
            .withColumn("cost", col("cost").cast(DecimalType(10, 2))) \
            .withColumn("profit_margin",
                       when(col("price") > 0,
                            spark_round((col("price") - col("cost")) / col("price") * 100, 2))
                       .otherwise(lit(0))) \
            .withColumn("price_tier",
                       when(col("price") < 20, "Budget")
                       .when(col("price") < 50, "Standard")
                       .when(col("price") < 100, "Premium")
                       .otherwise("Luxury")) \
            .withColumn("stock_quantity", 
                       coalesce(col("stock_quantity"), lit(0)).cast(IntegerType())) \
            .withColumn("stock_status",
                       when(col("stock_quantity") <= 0, "Out of Stock")
                       .when(col("stock_quantity") < 10, "Low Stock")
                       .when(col("stock_quantity") < 50, "Medium Stock")
                       .otherwise("In Stock")) \
            .withColumn("is_available", col("stock_quantity") > 0) \
            .select(
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "price",
                "cost",
                "profit_margin",
                "price_tier",
                "stock_quantity",
                "stock_status",
                "is_available"
            )
        
        return self._write_silver(cleaned, "clean_products", merge_key="product_id")
    
    def transform_orders(self) -> int:
        """
        Transform orders data.
        
        Transformations:
        - Standardize status and payment method
        - Create date dimensions
        - Calculate order value tiers
        - Add completion flags
        """
        print("🔄 Transforming orders...")
        
        raw_df = self._read_bronze("raw_orders")
        
        cleaned = raw_df \
            .withColumn("order_date", to_date(col("order_date"))) \
            .withColumn("order_timestamp", to_timestamp(col("order_date"))) \
            .withColumn("date_key", 
                       date_format(col("order_date"), "yyyyMMdd").cast(IntegerType())) \
            .withColumn("order_year", year(col("order_date"))) \
            .withColumn("order_month", month(col("order_date"))) \
            .withColumn("order_day", dayofmonth(col("order_date"))) \
            .withColumn("order_status", lower(trim(col("status")))) \
            .withColumn("payment_method", lower(trim(col("payment_method")))) \
            .withColumn("subtotal", 
                       coalesce(col("subtotal"), lit(0)).cast(DecimalType(12, 2))) \
            .withColumn("tax_amount", 
                       coalesce(col("tax"), lit(0)).cast(DecimalType(10, 2))) \
            .withColumn("shipping_amount", 
                       coalesce(col("shipping_cost"), lit(0)).cast(DecimalType(10, 2))) \
            .withColumn("total_amount", 
                       coalesce(col("total_amount"), lit(0)).cast(DecimalType(12, 2))) \
            .withColumn("order_value_tier",
                       when(col("total_amount") < 50, "Very Low")
                       .when(col("total_amount") < 100, "Low")
                       .when(col("total_amount") < 200, "Medium")
                       .when(col("total_amount") < 500, "High")
                       .otherwise("Very High")) \
            .withColumn("is_completed", lower(col("status")) == "completed") \
            .withColumn("is_shipped", 
                       lower(col("status")).isin("shipped", "completed")) \
            .withColumn("is_cancelled", lower(col("status")) == "cancelled") \
            .select(
                "order_id",
                "customer_id",
                "order_date",
                "order_timestamp",
                "date_key",
                "order_year",
                "order_month",
                "order_day",
                "order_status",
                "payment_method",
                "subtotal",
                "tax_amount",
                "shipping_amount",
                "total_amount",
                "order_value_tier",
                "is_completed",
                "is_shipped",
                "is_cancelled"
            )
        
        return self._write_silver(
            cleaned, 
            "clean_orders", 
            partition_by=["order_date"],
            merge_key="order_id"
        )
    
    def transform_order_items(self) -> int:
        """
        Transform order items data.
        
        Transformations:
        - Standardize amounts
        - Calculate line totals
        - Handle discounts
        """
        print("🔄 Transforming order items...")
        
        raw_df = self._read_bronze("raw_order_items")
        
        cleaned = raw_df \
            .withColumn("quantity", 
                       coalesce(col("quantity"), lit(1)).cast(IntegerType())) \
            .withColumn("unit_price", 
                       coalesce(col("unit_price"), lit(0)).cast(DecimalType(10, 2))) \
            .withColumn("discount_amount", 
                       coalesce(col("discount"), lit(0)).cast(DecimalType(10, 2))) \
            .withColumn("line_total",
                       col("quantity") * col("unit_price")) \
            .withColumn("net_amount",
                       col("line_total") - col("discount_amount")) \
            .withColumn("has_discount", col("discount_amount") > 0) \
            .select(
                "item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
                "discount_amount",
                "line_total",
                "net_amount",
                "has_discount"
            )
        
        return self._write_silver(cleaned, "clean_order_items")
    
    def run_all_transformations(self) -> Dict[str, int]:
        """
        Run all Silver layer transformations.
        
        Returns:
            Dictionary with row counts per table
        """
        print("\n" + "=" * 60)
        print("🥈 SILVER LAYER - Data Cleaning & Transformation")
        print("=" * 60)
        
        results = {}
        
        results['customers'] = self.transform_customers()
        results['products'] = self.transform_products()
        results['orders'] = self.transform_orders()
        results['order_items'] = self.transform_order_items()
        
        total = sum(results.values())
        print(f"\n   📊 Total rows processed: {total}")
        
        return results
    
    def read_table(self, table_name: str) -> DataFrame:
        """Read a silver table."""
        return self.manager.read_table(self.silver_db, table_name)
    
    def list_tables(self) -> List[str]:
        """List all silver tables."""
        return self.manager.list_tables(self.silver_db)
#!/usr/bin/env python3
"""
Spark Batch Processing Job for Orders
Processes order data in batch mode with comprehensive transformations
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, sum as spark_sum, count, avg,
    min as spark_min, max as spark_max, first, last,
    date_format, to_date, to_timestamp, datediff, months_between,
    year, month, dayofmonth, dayofweek, hour, minute,
    concat, concat_ws, upper, lower, trim, regexp_replace,
    split, explode, array, struct, map_from_arrays,
    row_number, rank, dense_rank, lag, lead,
    window, collect_list, collect_set,
    round as spark_round, ceil, floor, abs as spark_abs,
    current_timestamp, current_date, unix_timestamp,
    from_json, to_json, schema_of_json,
    broadcast, monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, BooleanType, ArrayType
)


class BatchOrdersProcessor:
    """Batch processor for order data using Apache Spark"""
    
    def __init__(self, spark: SparkSession = None):
        """Initialize the batch processor"""
        self.spark = spark or self._create_spark_session()
        self.processing_timestamp = datetime.now()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        return SparkSession.builder \
            .appName("BatchOrdersProcessing") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.jars.packages", 
                    "org.postgresql:postgresql:42.5.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.1") \
            .getOrCreate()
    
    def get_orders_schema(self) -> StructType:
        """Define schema for orders data"""
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_date", TimestampType(), False),
            StructField("status", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("billing_address", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])
    
    def get_order_items_schema(self) -> StructType:
        """Define schema for order items data"""
        return StructType([
            StructField("order_item_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount_percent", DoubleType(), True),
            StructField("tax_amount", DoubleType(), True)
        ])
    
    def read_orders_from_source(self, source_path: str, 
                                  format: str = "parquet") -> DataFrame:
        """Read orders data from source"""
        schema = self.get_orders_schema()
        
        if format == "parquet":
            return self.spark.read.parquet(source_path)
        elif format == "json":
            return self.spark.read.schema(schema).json(source_path)
        elif format == "csv":
            return self.spark.read.schema(schema).csv(source_path, header=True)
        elif format == "jdbc":
            return self._read_from_jdbc(source_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _read_from_jdbc(self, table_name: str) -> DataFrame:
        """Read data from PostgreSQL database"""
        jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/source_db")
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", os.getenv("DB_USER", "postgres")) \
            .option("password", os.getenv("DB_PASSWORD", "password")) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "10000") \
            .option("numPartitions", "10") \
            .load()
    
    def read_order_items(self, source_path: str, 
                          format: str = "parquet") -> DataFrame:
        """Read order items data"""
        schema = self.get_order_items_schema()
        
        if format == "parquet":
            return self.spark.read.parquet(source_path)
        elif format == "json":
            return self.spark.read.schema(schema).json(source_path)
        elif format == "csv":
            return self.spark.read.schema(schema).csv(source_path, header=True)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def read_products(self, source_path: str) -> DataFrame:
        """Read products dimension data"""
        return self.spark.read.parquet(source_path)
    
    def read_customers(self, source_path: str) -> DataFrame:
        """Read customers dimension data"""
        return self.spark.read.parquet(source_path)
    
    def clean_orders(self, orders_df: DataFrame) -> DataFrame:
        """Clean and validate orders data"""
        cleaned = orders_df \
            .filter(col("order_id").isNotNull()) \
            .filter(col("customer_id").isNotNull()) \
            .filter(col("order_date").isNotNull()) \
            .filter(col("total_amount") >= 0) \
            .dropDuplicates(["order_id"]) \
            .withColumn("order_id", trim(col("order_id"))) \
            .withColumn("customer_id", trim(col("customer_id"))) \
            .withColumn("status", 
                       coalesce(upper(trim(col("status"))), lit("UNKNOWN"))) \
            .withColumn("payment_method",
                       coalesce(trim(col("payment_method")), lit("UNKNOWN"))) \
            .withColumn("shipping_method",
                       coalesce(trim(col("shipping_method")), lit("STANDARD")))
        
        return cleaned
    
    def clean_order_items(self, items_df: DataFrame) -> DataFrame:
        """Clean and validate order items data"""
        cleaned = items_df \
            .filter(col("order_item_id").isNotNull()) \
            .filter(col("order_id").isNotNull()) \
            .filter(col("product_id").isNotNull()) \
            .filter(col("quantity") > 0) \
            .filter(col("unit_price") >= 0) \
            .dropDuplicates(["order_item_id"]) \
            .withColumn("discount_percent", 
                       coalesce(col("discount_percent"), lit(0.0))) \
            .withColumn("discount_percent",
                       when(col("discount_percent") > 100, 100.0)
                       .when(col("discount_percent") < 0, 0.0)
                       .otherwise(col("discount_percent"))) \
            .withColumn("tax_amount",
                       coalesce(col("tax_amount"), lit(0.0)))
        
        return cleaned
    
    def transform_order_items(self, items_df: DataFrame, 
                               products_df: DataFrame = None) -> DataFrame:
        """Transform order items with calculated fields"""
        # Calculate line item totals
        transformed = items_df \
            .withColumn("discount_amount",
                       spark_round(col("unit_price") * col("quantity") * 
                                   col("discount_percent") / 100, 2)) \
            .withColumn("subtotal",
                       spark_round(col("unit_price") * col("quantity"), 2)) \
            .withColumn("total_price",
                       spark_round(col("subtotal") - col("discount_amount") + 
                                   col("tax_amount"), 2))
        
        # Calculate profit if product cost is available
        if products_df is not None:
            transformed = transformed \
                .join(products_df.select("product_id", "cost"), "product_id", "left") \
                .withColumn("unit_cost", coalesce(col("cost"), col("unit_price") * 0.6)) \
                .withColumn("profit_amount",
                           spark_round((col("unit_price") - col("unit_cost")) * col("quantity") 
                                       - col("discount_amount"), 2)) \
                .withColumn("profit_margin",
                           when(col("total_price") > 0,
                                spark_round(col("profit_amount") / col("total_price") * 100, 2))
                           .otherwise(lit(0.0))) \
                .drop("cost")
        else:
            transformed = transformed \
                .withColumn("unit_cost", col("unit_price") * 0.6) \
                .withColumn("profit_amount",
                           spark_round((col("unit_price") * 0.4) * col("quantity") 
                                       - col("discount_amount"), 2)) \
                .withColumn("profit_margin",
                           when(col("total_price") > 0,
                                spark_round(col("profit_amount") / col("total_price") * 100, 2))
                           .otherwise(lit(0.0)))
        
        return transformed
    
    def enrich_orders(self, orders_df: DataFrame, 
                      customers_df: DataFrame = None) -> DataFrame:
        """Enrich orders with additional calculated fields"""
        enriched = orders_df \
            .withColumn("order_date_key",
                       date_format(col("order_date"), "yyyyMMdd").cast(IntegerType())) \
            .withColumn("order_year", year(col("order_date"))) \
            .withColumn("order_month", month(col("order_date"))) \
            .withColumn("order_day", dayofmonth(col("order_date"))) \
            .withColumn("order_dayofweek", dayofweek(col("order_date"))) \
            .withColumn("order_hour", hour(col("order_date"))) \
            .withColumn("is_weekend",
                       when(col("order_dayofweek").isin([1, 7]), True)
                       .otherwise(False)) \
            .withColumn("order_quarter", 
                       ceil(col("order_month") / 3).cast(IntegerType()))
        
        # Add order size tier
        enriched = enriched \
            .withColumn("order_size_tier",
                       when(col("total_amount") < 50, "Small")
                       .when(col("total_amount") < 100, "Medium")
                       .when(col("total_amount") < 250, "Large")
                       .when(col("total_amount") < 500, "XLarge")
                       .otherwise("Premium"))
        
        # Add status flags
        enriched = enriched \
            .withColumn("is_completed",
                       col("status").isin(["COMPLETED", "DELIVERED", "SHIPPED"])) \
            .withColumn("is_cancelled",
                       col("status").isin(["CANCELLED", "REFUNDED"])) \
            .withColumn("is_pending",
                       col("status").isin(["PENDING", "PROCESSING", "ON_HOLD"]))
        
        # Join with customer data if available
        if customers_df is not None:
            enriched = enriched \
                .join(broadcast(customers_df.select(
                    col("customer_id"),
                    col("customer_segment").alias("customer_segment"),
                    col("country").alias("customer_country"),
                    col("city").alias("customer_city"),
                    col("registration_date").alias("customer_since")
                )), "customer_id", "left") \
                .withColumn("customer_tenure_days",
                           datediff(col("order_date"), col("customer_since")))
        
        # Add processing metadata
        enriched = enriched \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("batch_id", lit(self.processing_timestamp.strftime("%Y%m%d%H%M%S")))
        
        return enriched
    
    def aggregate_order_items_to_orders(self, items_df: DataFrame) -> DataFrame:
        """Aggregate order items to order level"""
        order_aggregates = items_df.groupBy("order_id").agg(
            count("order_item_id").alias("item_count"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("subtotal").alias("items_subtotal"),
            spark_sum("discount_amount").alias("total_discount"),
            spark_sum("tax_amount").alias("total_tax"),
            spark_sum("total_price").alias("items_total"),
            spark_sum("profit_amount").alias("order_profit"),
            avg("profit_margin").alias("avg_profit_margin"),
            collect_set("product_id").alias("product_ids"),
            spark_min("unit_price").alias("min_item_price"),
            spark_max("unit_price").alias("max_item_price"),
            avg("unit_price").alias("avg_item_price")
        )
        
        return order_aggregates
    
    def calculate_customer_metrics(self, orders_df: DataFrame) -> DataFrame:
        """Calculate customer-level metrics from orders"""
        customer_window = Window.partitionBy("customer_id").orderBy("order_date")
        customer_all_window = Window.partitionBy("customer_id")
        
        # Calculate order sequence and RFM-like metrics
        customer_orders = orders_df \
            .withColumn("order_sequence", 
                       row_number().over(customer_window)) \
            .withColumn("previous_order_date",
                       lag("order_date").over(customer_window)) \
            .withColumn("days_since_last_order",
                       datediff(col("order_date"), col("previous_order_date"))) \
            .withColumn("is_repeat_customer",
                       col("order_sequence") > 1) \
            .withColumn("customer_lifetime_orders",
                       count("order_id").over(customer_all_window)) \
            .withColumn("customer_lifetime_value",
                       spark_sum("total_amount").over(customer_all_window)) \
            .withColumn("customer_avg_order_value",
                       avg("total_amount").over(customer_all_window))
        
        return customer_orders
    
    def create_daily_sales_summary(self, orders_df: DataFrame, 
                                   items_df: DataFrame = None) -> DataFrame:
        """Create daily sales summary aggregation"""
        # Aggregate orders by date
        daily_orders = orders_df \
            .withColumn("order_date_only", to_date(col("order_date"))) \
            .groupBy("order_date_only") \
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                spark_sum(when(col("is_completed"), 1).otherwise(0))
                    .alias("completed_orders"),
                spark_sum(when(col("is_cancelled"), 1).otherwise(0))
                    .alias("cancelled_orders"),
                spark_sum(when(col("is_weekend"), col("total_amount")).otherwise(0))
                    .alias("weekend_revenue"),
                spark_sum(when(~col("is_weekend"), col("total_amount")).otherwise(0))
                    .alias("weekday_revenue"),
                spark_min("total_amount").alias("min_order_value"),
                spark_max("total_amount").alias("max_order_value")
            ) \
            .withColumn("completion_rate",
                       spark_round(col("completed_orders") / col("total_orders") * 100, 2)) \
            .withColumn("cancellation_rate",
                       spark_round(col("cancelled_orders") / col("total_orders") * 100, 2))
        
        # Add items aggregation if available
        if items_df is not None:
            daily_items = items_df \
                .join(orders_df.select("order_id", to_date(col("order_date")).alias("order_date_only")),
                      "order_id") \
                .groupBy("order_date_only") \
                .agg(
                    spark_sum("quantity").alias("total_items_sold"),
                    countDistinct("product_id").alias("unique_products_sold"),
                    spark_sum("profit_amount").alias("total_profit"),
                    avg("profit_margin").alias("avg_profit_margin")
                )
            
            daily_orders = daily_orders.join(daily_items, "order_date_only", "left")
        
        return daily_orders.orderBy("order_date_only")
    
    def create_product_performance(self, items_df: DataFrame,
                                    orders_df: DataFrame,
                                    products_df: DataFrame = None) -> DataFrame:
        """Create product performance summary"""
        # Join items with orders
        items_with_orders = items_df \
            .join(orders_df.select("order_id", "order_date", "status", "customer_id"),
                  "order_id")
        
        # Aggregate by product
        product_perf = items_with_orders \
            .groupBy("product_id") \
            .agg(
                count("order_item_id").alias("times_ordered"),
                spark_sum("quantity").alias("total_quantity_sold"),
                countDistinct("order_id").alias("unique_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                spark_sum("total_price").alias("total_revenue"),
                avg("unit_price").alias("avg_selling_price"),
                spark_sum("profit_amount").alias("total_profit"),
                avg("profit_margin").alias("avg_profit_margin"),
                spark_sum("discount_amount").alias("total_discounts_given"),
                spark_min("order_date").alias("first_order_date"),
                spark_max("order_date").alias("last_order_date")
            )
        
        # Calculate product rank
        product_perf = product_perf \
            .withColumn("revenue_rank",
                       dense_rank().over(Window.orderBy(col("total_revenue").desc()))) \
            .withColumn("quantity_rank",
                       dense_rank().over(Window.orderBy(col("total_quantity_sold").desc())))
        
        # Add product details if available
        if products_df is not None:
            product_perf = product_perf \
                .join(products_df.select(
                    "product_id", "name", "category", "brand"
                ), "product_id", "left")
        
        return product_perf
    
    def write_to_warehouse(self, df: DataFrame, table_name: str,
                           mode: str = "overwrite",
                           partition_cols: list = None):
        """Write DataFrame to data warehouse"""
        jdbc_url = os.getenv("WAREHOUSE_JDBC_URL", 
                            "jdbc:postgresql://localhost:5432/warehouse_db")
        
        writer = df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", os.getenv("WAREHOUSE_USER", "postgres")) \
            .option("password", os.getenv("WAREHOUSE_PASSWORD", "password")) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "10000") \
            .mode(mode)
        
        writer.save()
        print(f"Successfully wrote {df.count()} records to {table_name}")
    
    def write_to_data_lake(self, df: DataFrame, path: str,
                           format: str = "parquet",
                           partition_cols: list = None,
                           mode: str = "overwrite"):
        """Write DataFrame to data lake (S3/MinIO)"""
        writer = df.write.mode(mode)
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        if format == "parquet":
            writer.parquet(path)
        elif format == "delta":
            writer.format("delta").save(path)
        elif format == "json":
            writer.json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        print(f"Successfully wrote data to {path}")
    
    def process_orders_batch(self, orders_path: str, items_path: str,
                             products_path: str = None,
                             customers_path: str = None,
                             output_path: str = None,
                             write_to_db: bool = True):
        """Main batch processing pipeline for orders"""
        print(f"Starting batch processing at {self.processing_timestamp}")
        
        # Step 1: Read data
        print("Reading source data...")
        orders_df = self.read_orders_from_source(orders_path)
        items_df = self.read_order_items(items_path)
        products_df = self.read_products(products_path) if products_path else None
        customers_df = self.read_customers(customers_path) if customers_path else None
        
        # Step 2: Clean data
        print("Cleaning data...")
        orders_clean = self.clean_orders(orders_df)
        items_clean = self.clean_order_items(items_df)
        
        # Step 3: Transform data
        print("Transforming data...")
        items_transformed = self.transform_order_items(items_clean, products_df)
        orders_enriched = self.enrich_orders(orders_clean, customers_df)
        
        # Step 4: Aggregate order items to orders
        print("Aggregating data...")
        order_aggregates = self.aggregate_order_items_to_orders(items_transformed)
        orders_with_items = orders_enriched.join(order_aggregates, "order_id", "left")
        
        # Step 5: Calculate customer metrics
        print("Calculating customer metrics...")
        orders_final = self.calculate_customer_metrics(orders_with_items)
        
        # Step 6: Create summaries
        print("Creating summaries...")
        daily_summary = self.create_daily_sales_summary(orders_final, items_transformed)
        product_performance = self.create_product_performance(
            items_transformed, orders_final, products_df
        )
        
        # Step 7: Write outputs
        print("Writing outputs...")
        if output_path:
            self.write_to_data_lake(
                orders_final, 
                f"{output_path}/fact_orders",
                partition_cols=["order_year", "order_month"]
            )
            self.write_to_data_lake(
                items_transformed,
                f"{output_path}/fact_order_items"
            )
            self.write_to_data_lake(
                daily_summary,
                f"{output_path}/agg_daily_sales"
            )
            self.write_to_data_lake(
                product_performance,
                f"{output_path}/agg_product_performance"
            )
        
        if write_to_db:
            self.write_to_warehouse(orders_final, "fact_orders")
            self.write_to_warehouse(items_transformed, "fact_order_items")
            self.write_to_warehouse(daily_summary, "agg_daily_sales")
            self.write_to_warehouse(product_performance, "agg_product_performance")
        
        # Print summary
        print("\n=== Batch Processing Summary ===")
        print(f"Orders processed: {orders_final.count()}")
        print(f"Order items processed: {items_transformed.count()}")
        print(f"Days summarized: {daily_summary.count()}")
        print(f"Products analyzed: {product_performance.count()}")
        print(f"Completed at: {datetime.now()}")
        
        return {
            'orders': orders_final,
            'order_items': items_transformed,
            'daily_summary': daily_summary,
            'product_performance': product_performance
        }
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """Main entry point for batch processing job"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch Orders Processing Job')
    parser.add_argument('--orders-path', required=True, help='Path to orders data')
    parser.add_argument('--items-path', required=True, help='Path to order items data')
    parser.add_argument('--products-path', help='Path to products data')
    parser.add_argument('--customers-path', help='Path to customers data')
    parser.add_argument('--output-path', help='Output path for processed data')
    parser.add_argument('--write-to-db', action='store_true', help='Write to database')
    
    args = parser.parse_args()
    
    processor = BatchOrdersProcessor()
    
    try:
        results = processor.process_orders_batch(
            orders_path=args.orders_path,
            items_path=args.items_path,
            products_path=args.products_path,
            customers_path=args.customers_path,
            output_path=args.output_path,
            write_to_db=args.write_to_db
        )
        print("Batch processing completed successfully!")
    except Exception as e:
        print(f"Error during batch processing: {e}")
        raise
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
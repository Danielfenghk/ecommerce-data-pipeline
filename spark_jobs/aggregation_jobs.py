#!/usr/bin/env python3
"""
Spark Aggregation Jobs for Data Warehouse
Creates various aggregations and data marts for analytics
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, sum as spark_sum, count, avg, countDistinct,
    min as spark_min, max as spark_max, first, last,
    date_format, to_date, to_timestamp, datediff, months_between, add_months,
    year, month, dayofmonth, dayofweek, quarter, weekofyear,
    concat, concat_ws, upper, lower, trim,
    row_number, rank, dense_rank, lag, lead, ntile,
    percent_rank, cume_dist, collect_list, collect_set,
    round as spark_round, ceil, floor, abs as spark_abs,
    current_timestamp, current_date, date_sub, date_add,
    stddev, variance, skewness, kurtosis, corr,
    approx_count_distinct, percentile_approx,
    broadcast, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, BooleanType
)


class AggregationJobs:
    """Aggregation jobs for creating data marts and summaries"""
    
    def __init__(self, spark: SparkSession = None):
        """Initialize aggregation jobs"""
        self.spark = spark or self._create_spark_session()
        self.processing_date = datetime.now()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        return SparkSession.builder \
            .appName("AggregationJobs") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.jars.packages",
                    "org.postgresql:postgresql:42.5.0") \
            .getOrCreate()
    
    def read_from_warehouse(self, table_name: str) -> DataFrame:
        """Read data from PostgreSQL data warehouse"""
        jdbc_url = os.getenv("WAREHOUSE_JDBC_URL",
                            "jdbc:postgresql://localhost:5432/warehouse_db")
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", os.getenv("WAREHOUSE_USER", "postgres")) \
            .option("password", os.getenv("WAREHOUSE_PASSWORD", "password")) \
            .option("driver", "org.postgresql.Driver") \
            .load()
    
    def read_from_data_lake(self, path: str, format: str = "parquet") -> DataFrame:
        """Read data from data lake"""
        if format == "parquet":
            return self.spark.read.parquet(path)
        elif format == "delta":
            return self.spark.read.format("delta").load(path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def write_to_warehouse(self, df: DataFrame, table_name: str,
                           mode: str = "overwrite"):
        """Write aggregation to PostgreSQL"""
        jdbc_url = os.getenv("WAREHOUSE_JDBC_URL",
                            "jdbc:postgresql://localhost:5432/warehouse_db")
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", os.getenv("WAREHOUSE_USER", "postgres")) \
            .option("password", os.getenv("WAREHOUSE_PASSWORD", "password")) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()
        
        print(f"Wrote {df.count()} records to {table_name}")
    
    # ===== Daily Aggregations =====
    
    def create_daily_sales_mart(self, orders_df: DataFrame,
                                 order_items_df: DataFrame = None) -> DataFrame:
        """Create daily sales data mart"""
        daily_sales = orders_df \
            .withColumn("order_date", to_date(col("order_date"))) \
            .groupBy("order_date") \
            .agg(
                # Order metrics
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                spark_sum(when(col("is_completed"), 1).otherwise(0))
                    .alias("completed_orders"),
                spark_sum(when(col("is_cancelled"), 1).otherwise(0))
                    .alias("cancelled_orders"),
                
                # Revenue metrics
                spark_sum("total_amount").alias("gross_revenue"),
                spark_sum(when(col("is_completed"), col("total_amount")).otherwise(0))
                    .alias("net_revenue"),
                avg("total_amount").alias("avg_order_value"),
                spark_min("total_amount").alias("min_order_value"),
                spark_max("total_amount").alias("max_order_value"),
                percentile_approx("total_amount", 0.5).alias("median_order_value"),
                
                # Customer metrics
                spark_sum(when(col("is_repeat_customer"), 1).otherwise(0))
                    .alias("repeat_customers"),
                spark_sum(when(~col("is_repeat_customer"), 1).otherwise(0))
                    .alias("new_customers"),
                
                # Items metrics
                spark_sum("item_count").alias("total_items_sold"),
                spark_sum("total_quantity").alias("total_units_sold"),
                
                # Profit metrics
                spark_sum("order_profit").alias("total_profit"),
                avg("avg_profit_margin").alias("avg_profit_margin"),
                
                # Order size distribution
                spark_sum(when(col("order_size_tier") == "Small", 1).otherwise(0))
                    .alias("small_orders"),
                spark_sum(when(col("order_size_tier") == "Medium", 1).otherwise(0))
                    .alias("medium_orders"),
                spark_sum(when(col("order_size_tier") == "Large", 1).otherwise(0))
                    .alias("large_orders"),
                spark_sum(when(col("order_size_tier") == "Premium", 1).otherwise(0))
                    .alias("premium_orders"),
                
                # Time-based metrics
                spark_sum(when(col("is_weekend"), 1).otherwise(0))
                    .alias("weekend_orders"),
                spark_sum(when(~col("is_weekend"), 1).otherwise(0))
                    .alias("weekday_orders")
            )
        
        # Calculate derived metrics
        daily_sales = daily_sales \
            .withColumn("completion_rate",
                       spark_round(col("completed_orders") / col("total_orders") * 100, 2)) \
            .withColumn("cancellation_rate",
                       spark_round(col("cancelled_orders") / col("total_orders") * 100, 2)) \
            .withColumn("repeat_customer_rate",
                       when(col("unique_customers") > 0,
                            spark_round(col("repeat_customers") / col("unique_customers") * 100, 2))
                       .otherwise(0)) \
            .withColumn("items_per_order",
                       spark_round(col("total_items_sold") / col("total_orders"), 2)) \
            .withColumn("profit_margin",
                       when(col("net_revenue") > 0,
                            spark_round(col("total_profit") / col("net_revenue") * 100, 2))
                       .otherwise(0)) \
            .withColumn("year", year(col("order_date"))) \
            .withColumn("month", month(col("order_date"))) \
            .withColumn("day", dayofmonth(col("order_date"))) \
            .withColumn("day_of_week", dayofweek(col("order_date"))) \
            .withColumn("week_of_year", weekofyear(col("order_date"))) \
            .withColumn("quarter", quarter(col("order_date"))) \
            .withColumn("is_weekend", dayofweek(col("order_date")).isin([1, 7])) \
            .withColumn("created_at", current_timestamp())
        
        return daily_sales.orderBy("order_date")
    
    def create_weekly_sales_mart(self, daily_sales_df: DataFrame) -> DataFrame:
        """Create weekly sales aggregation from daily data"""
        weekly_sales = daily_sales_df \
            .withColumn("week_start", 
                       date_sub(col("order_date"), dayofweek(col("order_date")) - 1)) \
            .groupBy("year", "week_of_year", "week_start") \
            .agg(
                count("*").alias("days_with_orders"),
                spark_sum("total_orders").alias("total_orders"),
                spark_sum("unique_customers").alias("total_customers"),
                spark_sum("gross_revenue").alias("gross_revenue"),
                spark_sum("net_revenue").alias("net_revenue"),
                avg("avg_order_value").alias("avg_order_value"),
                spark_sum("total_items_sold").alias("total_items_sold"),
                spark_sum("total_profit").alias("total_profit"),
                spark_sum("completed_orders").alias("completed_orders"),
                spark_sum("cancelled_orders").alias("cancelled_orders"),
                spark_sum("new_customers").alias("new_customers"),
                spark_sum("repeat_customers").alias("repeat_customers")
            ) \
            .withColumn("completion_rate",
                       spark_round(col("completed_orders") / col("total_orders") * 100, 2)) \
            .withColumn("avg_daily_orders",
                       spark_round(col("total_orders") / col("days_with_orders"), 2)) \
            .withColumn("avg_daily_revenue",
                       spark_round(col("gross_revenue") / col("days_with_orders"), 2)) \
            .withColumn("created_at", current_timestamp())
        
        return weekly_sales.orderBy("year", "week_of_year")
    
    def create_monthly_sales_mart(self, daily_sales_df: DataFrame) -> DataFrame:
        """Create monthly sales aggregation"""
        monthly_sales = daily_sales_df \
            .groupBy("year", "month") \
            .agg(
                count("*").alias("days_with_orders"),
                spark_sum("total_orders").alias("total_orders"),
                spark_sum("unique_customers").alias("total_customers"),
                spark_sum("gross_revenue").alias("gross_revenue"),
                spark_sum("net_revenue").alias("net_revenue"),
                avg("avg_order_value").alias("avg_order_value"),
                spark_sum("total_items_sold").alias("total_items_sold"),
                spark_sum("total_profit").alias("total_profit"),
                spark_sum("completed_orders").alias("completed_orders"),
                spark_sum("new_customers").alias("new_customers"),
                spark_sum("repeat_customers").alias("repeat_customers"),
                spark_min("order_date").alias("month_start"),
                spark_max("order_date").alias("month_end")
            )
        
        # Add period-over-period comparisons
        window_spec = Window.orderBy("year", "month")
        
        monthly_sales = monthly_sales \
            .withColumn("prev_month_revenue", 
                       lag("gross_revenue").over(window_spec)) \
            .withColumn("prev_month_orders",
                       lag("total_orders").over(window_spec)) \
            .withColumn("revenue_mom_growth",
                       when(col("prev_month_revenue") > 0,
                            spark_round((col("gross_revenue") - col("prev_month_revenue")) / 
                                        col("prev_month_revenue") * 100, 2))
                       .otherwise(0)) \
            .withColumn("orders_mom_growth",
                       when(col("prev_month_orders") > 0,
                            spark_round((col("total_orders") - col("prev_month_orders")) / 
                                        col("prev_month_orders") * 100, 2))
                       .otherwise(0)) \
            .withColumn("created_at", current_timestamp())
        
        return monthly_sales.orderBy("year", "month")
    
    # ===== Customer Aggregations =====
    
    def create_customer_rfm_mart(self, orders_df: DataFrame,
                                   analysis_date: str = None) -> DataFrame:
        """Create RFM (Recency, Frequency, Monetary) analysis mart"""
        if analysis_date is None:
            analysis_date = datetime.now().strftime("%Y-%m-%d")
        
        analysis_date_col = to_date(lit(analysis_date))
        
        # Calculate RFM values
        rfm_base = orders_df \
            .filter(col("is_completed")) \
            .groupBy("customer_id") \
            .agg(
                datediff(analysis_date_col, spark_max(col("order_date"))).alias("recency"),
                count("order_id").alias("frequency"),
                spark_sum("total_amount").alias("monetary"),
                spark_min("order_date").alias("first_order_date"),
                spark_max("order_date").alias("last_order_date"),
                avg("total_amount").alias("avg_order_value")
            )
        
        # Create RFM scores (1-5)
        rfm_scored = rfm_base \
            .withColumn("recency_score",
                       when(col("recency") <= 30, 5)
                       .when(col("recency") <= 60, 4)
                       .when(col("recency") <= 90, 3)
                       .when(col("recency") <= 180, 2)
                       .otherwise(1)) \
            .withColumn("frequency_score",
                       when(col("frequency") >= 20, 5)
                       .when(col("frequency") >= 10, 4)
                       .when(col("frequency") >= 5, 3)
                       .when(col("frequency") >= 2, 2)
                       .otherwise(1)) \
            .withColumn("monetary_score",
                       when(col("monetary") >= 1000, 5)
                       .when(col("monetary") >= 500, 4)
                       .when(col("monetary") >= 200, 3)
                       .when(col("monetary") >= 50, 2)
                       .otherwise(1))
        
        # Calculate RFM segment
        rfm_final = rfm_scored \
            .withColumn("rfm_score",
                       col("recency_score") + col("frequency_score") + col("monetary_score")) \
            .withColumn("rfm_string",
                       concat(col("recency_score"), col("frequency_score"), col("monetary_score"))) \
            .withColumn("customer_segment",
                       when(col("rfm_score") >= 12, "Champions")
                       .when((col("rfm_score") >= 9) & (col("recency_score") >= 4), "Loyal Customers")
                       .when((col("rfm_score") >= 9) & (col("recency_score") < 4), "At Risk")
                       .when((col("frequency_score") >= 4) & (col("recency_score") <= 2), "Can't Lose Them")
                       .when((col("rfm_score") >= 6) & (col("recency_score") >= 3), "Potential Loyalists")
                       .when((col("frequency_score") == 1) & (col("recency_score") >= 4), "New Customers")
                       .when(col("rfm_score") <= 5, "Lost")
                       .otherwise("Others")) \
            .withColumn("analysis_date", analysis_date_col) \
            .withColumn("created_at", current_timestamp())
        
        return rfm_final
    
    def create_customer_lifetime_value_mart(self, orders_df: DataFrame) -> DataFrame:
        """Calculate Customer Lifetime Value (CLV)"""
        # Aggregate customer data
        customer_data = orders_df \
            .filter(col("is_completed")) \
            .groupBy("customer_id") \
            .agg(
                spark_sum("total_amount").alias("total_revenue"),
                count("order_id").alias("total_orders"),
                avg("total_amount").alias("avg_order_value"),
                spark_min("order_date").alias("first_order_date"),
                spark_max("order_date").alias("last_order_date"),
                months_between(spark_max("order_date"), spark_min("order_date")).alias("tenure_months")
            )
        
        # Calculate CLV metrics
        clv_mart = customer_data \
            .withColumn("tenure_months", 
                       when(col("tenure_months") == 0, 1).otherwise(col("tenure_months"))) \
            .withColumn("purchase_frequency",
                       col("total_orders") / col("tenure_months")) \
            .withColumn("monthly_revenue",
                       col("total_revenue") / col("tenure_months")) \
            .withColumn("clv_12m",
                       spark_round(col("monthly_revenue") * 12, 2)) \
            .withColumn("clv_24m",
                       spark_round(col("monthly_revenue") * 24, 2)) \
            .withColumn("clv_tier",
                       when(col("clv_12m") >= 5000, "Platinum")
                       .when(col("clv_12m") >= 2000, "Gold")
                       .when(col("clv_12m") >= 500, "Silver")
                       .otherwise("Bronze")) \
            .withColumn("days_since_last_order",
                       datediff(current_date(), col("last_order_date"))) \
            .withColumn("is_active",
                       col("days_since_last_order") <= 90) \
            .withColumn("created_at", current_timestamp())
        
        return clv_mart
    
    # ===== Product Aggregations =====
    
    def create_product_performance_mart(self, order_items_df: DataFrame,
                                         products_df: DataFrame = None) -> DataFrame:
        """Create product performance data mart"""
        product_perf = order_items_df \
            .groupBy("product_id") \
            .agg(
                count("order_item_id").alias("times_ordered"),
                spark_sum("quantity").alias("total_quantity_sold"),
                countDistinct("order_id").alias("unique_orders"),
                spark_sum("total_price").alias("total_revenue"),
                spark_sum("profit_amount").alias("total_profit"),
                avg("unit_price").alias("avg_selling_price"),
                avg("discount_percent").alias("avg_discount_given"),
                spark_sum("discount_amount").alias("total_discount_amount"),
                avg("profit_margin").alias("avg_profit_margin"),
                spark_min("unit_price").alias("min_selling_price"),
                spark_max("unit_price").alias("max_selling_price")
            )
        
        # Add ranking
        product_perf = product_perf \
            .withColumn("revenue_rank",
                       dense_rank().over(Window.orderBy(col("total_revenue").desc()))) \
            .withColumn("quantity_rank",
                       dense_rank().over(Window.orderBy(col("total_quantity_sold").desc()))) \
            .withColumn("profit_rank",
                       dense_rank().over(Window.orderBy(col("total_profit").desc()))) \
            .withColumn("revenue_percentile",
                       ntile(100).over(Window.orderBy(col("total_revenue")))) \
            .withColumn("is_top_seller", col("revenue_rank") <= 100) \
            .withColumn("is_slow_mover", col("quantity_rank") > 
                       (spark_max(col("quantity_rank")).over(Window.partitionBy()) * 0.9))
        
        # Join with product details if available
        if products_df is not None:
            product_perf = product_perf.join(
                products_df.select("product_id", "name", "category", "brand", "cost"),
                "product_id",
                "left"
            )
        
        product_perf = product_perf \
            .withColumn("created_at", current_timestamp())
        
        return product_perf
    
    def create_category_performance_mart(self, order_items_df: DataFrame,
                                          products_df: DataFrame) -> DataFrame:
        """Create category-level performance aggregation"""
        # Join items with products to get category
        items_with_category = order_items_df.join(
            products_df.select("product_id", "category"),
            "product_id",
            "left"
        )
        
        category_perf = items_with_category \
            .groupBy("category") \
            .agg(
                countDistinct("product_id").alias("products_count"),
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_sum("total_price").alias("total_revenue"),
                spark_sum("profit_amount").alias("total_profit"),
                countDistinct("order_id").alias("total_orders"),
                avg("profit_margin").alias("avg_profit_margin"),
                avg("unit_price").alias("avg_item_price")
            ) \
            .withColumn("revenue_share",
                       spark_round(col("total_revenue") / 
                                   spark_sum("total_revenue").over(Window.partitionBy()) * 100, 2)) \
            .withColumn("category_rank",
                       dense_rank().over(Window.orderBy(col("total_revenue").desc()))) \
            .withColumn("created_at", current_timestamp())
        
        return category_perf
    
    # ===== Geographic Aggregations =====
    
    def create_geographic_sales_mart(self, orders_df: DataFrame,
                                      customers_df: DataFrame) -> DataFrame:
        """Create geographic sales aggregation"""
        orders_with_geo = orders_df.join(
            customers_df.select("customer_id", "country", "city", "region"),
            "customer_id",
            "left"
        )
        
        geo_sales = orders_with_geo \
            .groupBy("country", "region", "city") \
            .agg(
                count("order_id").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                spark_sum(when(col("is_completed"), 1).otherwise(0))
                    .alias("completed_orders")
            ) \
            .withColumn("completion_rate",
                       spark_round(col("completed_orders") / col("total_orders") * 100, 2)) \
            .withColumn("revenue_rank",
                       dense_rank().over(Window.orderBy(col("total_revenue").desc()))) \
            .withColumn("created_at", current_timestamp())
        
        return geo_sales
    
    # ===== Time-based Aggregations =====
    
    def create_hourly_patterns_mart(self, orders_df: DataFrame) -> DataFrame:
        """Create hourly ordering patterns analysis"""
        hourly_patterns = orders_df \
            .withColumn("order_hour", date_format(col("order_date"), "HH").cast(IntegerType())) \
            .withColumn("day_of_week", dayofweek(col("order_date"))) \
            .groupBy("order_hour", "day_of_week") \
            .agg(
                count("order_id").alias("total_orders"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_customers")
            ) \
            .withColumn("day_name",
                       when(col("day_of_week") == 1, "Sunday")
                       .when(col("day_of_week") == 2, "Monday")
                       .when(col("day_of_week") == 3, "Tuesday")
                       .when(col("day_of_week") == 4, "Wednesday")
                       .when(col("day_of_week") == 5, "Thursday")
                       .when(col("day_of_week") == 6, "Friday")
                       .otherwise("Saturday")) \
            .withColumn("is_business_hours",
                       (col("order_hour") >= 9) & (col("order_hour") <= 17)) \
            .withColumn("time_period",
                       when(col("order_hour") < 6, "Night")
                       .when(col("order_hour") < 12, "Morning")
                       .when(col("order_hour") < 18, "Afternoon")
                       .otherwise("Evening")) \
            .withColumn("created_at", current_timestamp())
        
        return hourly_patterns
    
    def run_all_aggregations(self, 
                              orders_path: str = None,
                              items_path: str = None,
                              products_path: str = None,
                              customers_path: str = None,
                              write_to_db: bool = True) -> Dict[str, DataFrame]:
        """Run all aggregation jobs"""
        print(f"Starting aggregation jobs at {self.processing_date}")
        
        # Read source data
        if orders_path:
            orders_df = self.read_from_data_lake(orders_path)
        else:
            orders_df = self.read_from_warehouse("fact_orders")
        
        if items_path:
            items_df = self.read_from_data_lake(items_path)
        else:
            items_df = self.read_from_warehouse("fact_order_items")
        
        if products_path:
            products_df = self.read_from_data_lake(products_path)
        else:
            products_df = self.read_from_warehouse("dim_products")
        
        if customers_path:
            customers_df = self.read_from_data_lake(customers_path)
        else:
            customers_df = self.read_from_warehouse("dim_customers")
        
        # Create aggregations
        results = {}
        
        print("Creating daily sales mart...")
        daily_sales = self.create_daily_sales_mart(orders_df, items_df)
        results['daily_sales'] = daily_sales
        
        print("Creating weekly sales mart...")
        weekly_sales = self.create_weekly_sales_mart(daily_sales)
        results['weekly_sales'] = weekly_sales
        
        print("Creating monthly sales mart...")
        monthly_sales = self.create_monthly_sales_mart(daily_sales)
        results['monthly_sales'] = monthly_sales
        
        print("Creating customer RFM mart...")
        customer_rfm = self.create_customer_rfm_mart(orders_df)
        results['customer_rfm'] = customer_rfm
        
        print("Creating customer CLV mart...")
        customer_clv = self.create_customer_lifetime_value_mart(orders_df)
        results['customer_clv'] = customer_clv
        
        print("Creating product performance mart...")
        product_perf = self.create_product_performance_mart(items_df, products_df)
        results['product_performance'] = product_perf
        
        print("Creating category performance mart...")
        category_perf = self.create_category_performance_mart(items_df, products_df)
        results['category_performance'] = category_perf
        
        print("Creating geographic sales mart...")
        geo_sales = self.create_geographic_sales_mart(orders_df, customers_df)
        results['geographic_sales'] = geo_sales
        
        print("Creating hourly patterns mart...")
        hourly_patterns = self.create_hourly_patterns_mart(orders_df)
        results['hourly_patterns'] = hourly_patterns
        
        # Write to warehouse
        if write_to_db:
            print("Writing results to warehouse...")
            self.write_to_warehouse(daily_sales, "mart_daily_sales")
            self.write_to_warehouse(weekly_sales, "mart_weekly_sales")
            self.write_to_warehouse(monthly_sales, "mart_monthly_sales")
            self.write_to_warehouse(customer_rfm, "mart_customer_rfm")
            self.write_to_warehouse(customer_clv, "mart_customer_clv")
            self.write_to_warehouse(product_perf, "mart_product_performance")
            self.write_to_warehouse(category_perf, "mart_category_performance")
            self.write_to_warehouse(geo_sales, "mart_geographic_sales")
            self.write_to_warehouse(hourly_patterns, "mart_hourly_patterns")
        
        print(f"Aggregation jobs completed at {datetime.now()}")
        
        return results
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """Main entry point for aggregation jobs"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Aggregation Jobs')
    parser.add_argument('--orders-path', help='Path to orders data')
    parser.add_argument('--items-path', help='Path to order items data')
    parser.add_argument('--products-path', help='Path to products data')
    parser.add_argument('--customers-path', help='Path to customers data')
    parser.add_argument('--no-db', action='store_true', help='Skip writing to database')
    
    args = parser.parse_args()
    
    aggregator = AggregationJobs()
    
    try:
        results = aggregator.run_all_aggregations(
            orders_path=args.orders_path,
            items_path=args.items_path,
            products_path=args.products_path,
            customers_path=args.customers_path,
            write_to_db=not args.no_db
        )
        
        print("\n=== Aggregation Summary ===")
        for name, df in results.items():
            print(f"{name}: {df.count()} records")
        
    except Exception as e:
        print(f"Error during aggregation: {e}")
        raise
    finally:
        aggregator.stop()


if __name__ == "__main__":
    main()
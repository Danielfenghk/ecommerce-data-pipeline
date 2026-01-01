"""
Gold Layer
==========

Business-level aggregations and analytics layer - creates dimensional
models, fact tables, and pre-aggregated analytics tables.
"""

from typing import Optional, List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, first, last,
    countDistinct, current_timestamp, lit, when,
    datediff, current_date, coalesce, row_number,
    percent_rank, ntile, dense_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

from .iceberg_manager import IcebergManager


class GoldLayer:
    """
    Gold Layer: Business Aggregations and Analytics
    
    Responsibilities:
    - Create dimensional models (dim_* tables)
    - Create fact tables (fact_* tables)
    - Build pre-aggregated analytics tables
    - Calculate business metrics
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "nessie"):
        """
        Initialize Gold Layer.
        
        Args:
            spark: SparkSession with Iceberg configured
            catalog: Iceberg catalog name
        """
        self.spark = spark
        self.catalog = catalog
        self.silver_db = "silver"
        self.gold_db = "gold"
        self.manager = IcebergManager(spark, catalog)
        
        # Ensure database exists
        self.manager.create_database(self.gold_db)
    
    def _read_silver(self, table_name: str) -> DataFrame:
        """Read table from Silver layer."""
        return self.manager.read_table(self.silver_db, table_name)
    
    def _write_gold(self, 
                    df: DataFrame, 
                    table_name: str,
                    partition_by: Optional[List[str]] = None) -> int:
        """
        Write DataFrame to Gold layer.
        
        Args:
            df: Aggregated DataFrame
            table_name: Target table name
            partition_by: Partitioning columns
        
        Returns:
            Number of rows written
        """
        # Add creation timestamp
        df = df.withColumn("_created_at", current_timestamp())
        
        full_table_name = f"{self.catalog}.{self.gold_db}.{table_name}"
        
        writer = df.writeTo(full_table_name) \
            .tableProperty("format-version", "2")
        
        if partition_by:
            writer = writer.partitionedBy(*partition_by)
        
        writer.createOrReplace()
        
        count = df.count()
        print(f"   ✅ Created {full_table_name} with {count} rows")
        return count
    
    def create_dim_customers(self) -> int:
        """
        Create customer dimension with aggregated metrics.
        
        Includes:
        - Customer attributes
        - Lifetime order metrics
        - Value tier classification
        - Activity status
        """
        print("🏗️ Creating dim_customers...")
        
        customers = self._read_silver("clean_customers")
        orders = self._read_silver("clean_orders")
        
        # Calculate customer order metrics
        customer_metrics = orders.groupBy("customer_id").agg(
            count("*").alias("lifetime_orders"),
            sum("total_amount").alias("lifetime_revenue"),
            avg("total_amount").alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            countDistinct("order_date").alias("distinct_order_days"),
            sum(when(col("is_completed"), 1).otherwise(0)).alias("completed_orders"),
            sum(when(col("is_cancelled"), 1).otherwise(0)).alias("cancelled_orders")
        )
        
        # Join customers with metrics
        dim = customers.join(customer_metrics, on="customer_id", how="left") \
            .withColumn("lifetime_orders", 
                       coalesce(col("lifetime_orders"), lit(0))) \
            .withColumn("lifetime_revenue",
                       coalesce(col("lifetime_revenue"), lit(0.0))) \
            .withColumn("avg_order_value",
                       coalesce(col("avg_order_value"), lit(0.0))) \
            .withColumn("completed_orders",
                       coalesce(col("completed_orders"), lit(0))) \
            .withColumn("cancelled_orders",
                       coalesce(col("cancelled_orders"), lit(0))) \
            .withColumn("days_since_last_order",
                       when(col("last_order_date").isNotNull(),
                            datediff(current_date(), col("last_order_date")))
                       .otherwise(lit(9999))) \
            .withColumn("days_since_first_order",
                       when(col("first_order_date").isNotNull(),
                            datediff(current_date(), col("first_order_date")))
                       .otherwise(lit(0))) \
            .withColumn("customer_value_tier",
                       when(col("lifetime_revenue") >= 1000, "Platinum")
                       .when(col("lifetime_revenue") >= 500, "Gold")
                       .when(col("lifetime_revenue") >= 200, "Silver")
                       .when(col("lifetime_revenue") > 0, "Bronze")
                       .otherwise("Prospect")) \
            .withColumn("activity_status",
                       when(col("days_since_last_order") <= 30, "Active")
                       .when(col("days_since_last_order") <= 90, "At Risk")
                       .when(col("days_since_last_order") <= 365, "Dormant")
                       .when(col("lifetime_orders") == 0, "Never Purchased")
                       .otherwise("Churned")) \
            .withColumn("completion_rate",
                       when(col("lifetime_orders") > 0,
                            col("completed_orders") / col("lifetime_orders") * 100)
                       .otherwise(lit(0.0))) \
            .withColumn("cancellation_rate",
                       when(col("lifetime_orders") > 0,
                            col("cancelled_orders") / col("lifetime_orders") * 100)
                       .otherwise(lit(0.0)))
        
        return self._write_gold(dim, "dim_customers")
    
    def create_dim_products(self) -> int:
        """
        Create product dimension with sales metrics.
        
        Includes:
        - Product attributes
        - Sales performance metrics
        - Product ranking
        """
        print("🏗️ Creating dim_products...")
        
        products = self._read_silver("clean_products")
        order_items = self._read_silver("clean_order_items")
        
        # Calculate product sales metrics
        product_metrics = order_items.groupBy("product_id").agg(
            sum("quantity").alias("total_quantity_sold"),
            sum("net_amount").alias("total_revenue"),
            count("*").alias("times_ordered"),
            avg("quantity").alias("avg_quantity_per_order"),
            avg("unit_price").alias("avg_selling_price"),
            countDistinct("order_id").alias("unique_orders")
        )
        
        # Window for ranking
        revenue_window = Window.orderBy(col("total_revenue").desc())
        quantity_window = Window.orderBy(col("total_quantity_sold").desc())
        
        # Join and add rankings
        dim = products.join(product_metrics, on="product_id", how="left") \
            .withColumn("total_quantity_sold",
                       coalesce(col("total_quantity_sold"), lit(0))) \
            .withColumn("total_revenue",
                       coalesce(col("total_revenue"), lit(0.0))) \
            .withColumn("times_ordered",
                       coalesce(col("times_ordered"), lit(0))) \
            .withColumn("revenue_rank",
                       dense_rank().over(revenue_window)) \
            .withColumn("quantity_rank",
                       dense_rank().over(quantity_window)) \
            .withColumn("revenue_percentile",
                       percent_rank().over(revenue_window)) \
            .withColumn("performance_tier",
                       when(col("revenue_percentile") <= 0.1, "Top 10%")
                       .when(col("revenue_percentile") <= 0.25, "Top 25%")
                       .when(col("revenue_percentile") <= 0.5, "Top 50%")
                       .otherwise("Bottom 50%"))
        
        return self._write_gold(dim, "dim_products")
    
    def create_dim_date(self) -> int:
        """
        Create date dimension table.
        
        Includes standard date attributes for analytics.
        """
        print("🏗️ Creating dim_date...")
        
        from pyspark.sql.functions import (
            explode, sequence, to_date, dayofweek, weekofyear,
            quarter, dayofyear
        )
        
        # Generate date range
        date_df = self.spark.sql("""
            SELECT explode(sequence(
                to_date('2023-01-01'), 
                to_date('2025-12-31'), 
                interval 1 day
            )) as full_date
        """)
        
        dim = date_df \
            .withColumn("date_key", 
                       date_format(col("full_date"), "yyyyMMdd").cast("int")) \
            .withColumn("year", year(col("full_date"))) \
            .withColumn("quarter", quarter(col("full_date"))) \
            .withColumn("month", month(col("full_date"))) \
            .withColumn("month_name", date_format(col("full_date"), "MMMM")) \
            .withColumn("week_of_year", weekofyear(col("full_date"))) \
            .withColumn("day_of_month", dayofmonth(col("full_date"))) \
            .withColumn("day_of_week", dayofweek(col("full_date"))) \
            .withColumn("day_name", date_format(col("full_date"), "EEEE")) \
            .withColumn("day_of_year", dayofyear(col("full_date"))) \
            .withColumn("is_weekend", dayofweek(col("full_date")).isin(1, 7)) \
            .withColumn("is_month_start", dayofmonth(col("full_date")) == 1) \
            .withColumn("is_month_end", 
                       col("full_date") == last_day(col("full_date")))
        
        return self._write_gold(dim, "dim_date")
    
    def create_fact_orders(self) -> int:
        """
        Create orders fact table.
        
        Includes order details with foreign keys to dimensions.
        """
        print("🏗️ Creating fact_orders...")
        
        orders = self._read_silver("clean_orders")
        order_items = self._read_silver("clean_order_items")
        
        # Aggregate order items
        item_summary = order_items.groupBy("order_id").agg(
            count("*").alias("line_item_count"),
            sum("quantity").alias("total_quantity"),
            sum("discount_amount").alias("total_discount"),
            countDistinct("product_id").alias("unique_products")
        )
        
        # Join orders with item summary
        fact = orders.join(item_summary, on="order_id", how="left") \
            .withColumn("line_item_count",
                       coalesce(col("line_item_count"), lit(0))) \
            .withColumn("total_quantity",
                       coalesce(col("total_quantity"), lit(0))) \
            .withColumn("total_discount",
                       coalesce(col("total_discount"), lit(0.0))) \
            .withColumn("unique_products",
                       coalesce(col("unique_products"), lit(0))) \
            .withColumn("net_revenue",
                       col("total_amount") - col("total_discount"))
        
        return self._write_gold(fact, "fact_orders", partition_by=["order_date"])
    
    def create_fact_order_items(self) -> int:
        """Create order items fact table."""
        print("🏗️ Creating fact_order_items...")
        
        order_items = self._read_silver("clean_order_items")
        orders = self._read_silver("clean_orders").select(
            "order_id", "customer_id", "order_date", "date_key"
        )
        
        # Join to get order context
        fact = order_items.join(orders, on="order_id", how="inner")
        
        return self._write_gold(fact, "fact_order_items", partition_by=["order_date"])
    
    def create_daily_sales_summary(self) -> int:
        """
        Create daily sales summary aggregation.
        
        Pre-aggregated metrics for dashboard performance.
        """
        print("🏗️ Creating daily_sales_summary...")
        
        orders = self._read_silver("clean_orders")
        
        daily = orders.groupBy("order_date", "date_key").agg(
            count("*").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("subtotal").alias("gross_sales"),
            sum("total_amount").alias("net_revenue"),
            sum("tax_amount").alias("total_tax"),
            sum("shipping_amount").alias("total_shipping"),
            avg("total_amount").alias("avg_order_value"),
            max("total_amount").alias("max_order_value"),
            min("total_amount").alias("min_order_value"),
            sum(when(col("is_completed"), 1).otherwise(0)).alias("completed_orders"),
            sum(when(col("is_cancelled"), 1).otherwise(0)).alias("cancelled_orders"),
            sum(when(col("order_value_tier") == "High", 1).otherwise(0)).alias("high_value_orders"),
            sum(when(col("order_value_tier") == "Very High", 1).otherwise(0)).alias("very_high_value_orders")
        ).withColumn("completion_rate",
                    when(col("total_orders") > 0,
                         col("completed_orders") / col("total_orders") * 100)
                    .otherwise(0)) \
         .withColumn("cancellation_rate",
                    when(col("total_orders") > 0,
                         col("cancelled_orders") / col("total_orders") * 100)
                    .otherwise(0)) \
         .orderBy("order_date")
        
        return self._write_gold(daily, "daily_sales_summary")
    
    def create_customer_rfm(self) -> int:
        """
        Create RFM (Recency, Frequency, Monetary) analysis table.
        
        Used for customer segmentation and targeting.
        """
        print("🏗️ Creating customer_rfm...")
        
        # Read from Gold dim_customers (already has metrics)
        customers = self.manager.read_table(self.gold_db, "dim_customers")
        
        rfm = customers.select(
            "customer_id",
            "full_name",
            "email",
            "customer_segment",
            "days_since_last_order",
            "lifetime_orders",
            "lifetime_revenue",
            # Recency Score (1-5, 5 = most recent)
            when(col("days_since_last_order") <= 7, 5)
            .when(col("days_since_last_order") <= 30, 4)
            .when(col("days_since_last_order") <= 90, 3)
            .when(col("days_since_last_order") <= 180, 2)
            .otherwise(1).alias("recency_score"),
            # Frequency Score (1-5, 5 = most frequent)
            when(col("lifetime_orders") >= 20, 5)
            .when(col("lifetime_orders") >= 10, 4)
            .when(col("lifetime_orders") >= 5, 3)
            .when(col("lifetime_orders") >= 2, 2)
            .otherwise(1).alias("frequency_score"),
            # Monetary Score (1-5, 5 = highest value)
            when(col("lifetime_revenue") >= 1000, 5)
            .when(col("lifetime_revenue") >= 500, 4)
            .when(col("lifetime_revenue") >= 200, 3)
            .when(col("lifetime_revenue") >= 50, 2)
            .otherwise(1).alias("monetary_score")
        ).withColumn("rfm_score",
                    col("recency_score") + col("frequency_score") + col("monetary_score")) \
         .withColumn("rfm_segment",
                    when(col("rfm_score") >= 13, "Champions")
                    .when(col("rfm_score") >= 10, "Loyal Customers")
                    .when(col("rfm_score") >= 7, "Potential Loyalists")
                    .when(col("rfm_score") >= 4, "At Risk")
                    .otherwise("Need Attention")) \
         .withColumn("marketing_action",
                    when(col("rfm_segment") == "Champions", "Loyalty rewards, early access")
                    .when(col("rfm_segment") == "Loyal Customers", "Upsell, cross-sell")
                    .when(col("rfm_segment") == "Potential Loyalists", "Nurture campaigns")
                    .when(col("rfm_segment") == "At Risk", "Win-back campaigns")
                    .otherwise("Re-engagement emails"))
        
        return self._write_gold(rfm, "customer_rfm")
    
    def create_product_performance(self) -> int:
        """Create product performance analytics table."""
        print("🏗️ Creating product_performance...")
        
        products = self.manager.read_table(self.gold_db, "dim_products")
        order_items = self._read_silver("clean_order_items")
        orders = self._read_silver("clean_orders")
        
        # Join order items with orders for date context
        items_with_date = order_items.join(
            orders.select("order_id", "order_date", "order_month"),
            on="order_id"
        )
        
        # Monthly product performance
        monthly_perf = items_with_date.groupBy(
            "product_id", 
            year(col("order_date")).alias("year"),
            col("order_month").alias("month")
        ).agg(
            sum("quantity").alias("monthly_quantity"),
            sum("net_amount").alias("monthly_revenue"),
            count("*").alias("monthly_orders")
        )
        
        # Get product info and aggregate
        performance = products.select(
            "product_id", "product_name", "category", "brand",
            "total_quantity_sold", "total_revenue", "revenue_rank"
        ).join(
            monthly_perf.groupBy("product_id").agg(
                avg("monthly_revenue").alias("avg_monthly_revenue"),
                max("monthly_revenue").alias("best_month_revenue"),
                min("monthly_revenue").alias("worst_month_revenue")
            ),
            on="product_id",
            how="left"
        )
        
        return self._write_gold(performance, "product_performance")
    
    def run_all_aggregations(self) -> Dict[str, int]:
        """
        Run all Gold layer aggregations.
        
        Returns:
            Dictionary with row counts per table
        """
        print("\n" + "=" * 60)
        print("🥇 GOLD LAYER - Business Aggregations")
        print("=" * 60)
        
        results = {}
        
        # Create dimension tables first
        results['dim_customers'] = self.create_dim_customers()
        results['dim_products'] = self.create_dim_products()
        
        # Create fact tables
        results['fact_orders'] = self.create_fact_orders()
        results['fact_order_items'] = self.create_fact_order_items()
        
        # Create analytics tables
        results['daily_sales'] = self.create_daily_sales_summary()
        results['customer_rfm'] = self.create_customer_rfm()
        results['product_perf'] = self.create_product_performance()
        
        total = sum(results.values())
        print(f"\n   📊 Total rows created: {total}")
        
        return results
    
    def read_table(self, table_name: str) -> DataFrame:
        """Read a gold table."""
        return self.manager.read_table(self.gold_db, table_name)
    
    def list_tables(self) -> List[str]:
        """List all gold tables."""
        return self.manager.list_tables(self.gold_db)
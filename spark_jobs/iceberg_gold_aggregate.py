"""
Gold Layer - Business Aggregations and Marts
建立分析就緒的聚合表格
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, 
    countDistinct, current_timestamp,
    first, last, datediff, current_date
)

from iceberg_config import get_spark_iceberg_session


class GoldLayerAggregate:
    """Gold Layer: 商業指標聚合表"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.catalog = "nessie"
        self.silver_db = "silver"
        self.gold_db = "gold"
    
    def create_database(self):
        """建立 Gold 資料庫"""
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.gold_db}")
        print(f" Created database: {self.catalog}.{self.gold_db}")
    
    def create_dim_customers(self):
        """
        建立客戶維度表 (包含訂單彙總)
        """
        print("🏗️ Creating dim_customers...")
        
        customers = self.spark.table(f"{self.catalog}.{self.silver_db}.cleaned_customers")
        orders = self.spark.table(f"{self.catalog}.{self.silver_db}.cleaned_orders")
        
        # 計算客戶訂單指標
        customer_metrics = orders.groupBy("customer_id").agg(
            count("*").alias("lifetime_orders"),
            sum("total_amount").alias("lifetime_revenue"),
            avg("total_amount").alias("avg_order_value"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            countDistinct("order_date").alias("active_days")
        )
        
        # 合併客戶資料與訂單指標
        dim_customers = customers.join(
            customer_metrics, 
            on="customer_id", 
            how="left"
        ).withColumn("days_since_last_order",
                    datediff(current_date(), col("last_order_date"))) \
         .withColumn("customer_value_tier",
                    when(col("lifetime_revenue") >= 1000, "High Value")
                    .when(col("lifetime_revenue") >= 500, "Medium Value")
                    .when(col("lifetime_revenue") > 0, "Low Value")
                    .otherwise("No Purchases")) \
         .withColumn("activity_status",
                    when(col("days_since_last_order") <= 30, "Active")
                    .when(col("days_since_last_order") <= 90, "At Risk")
                    .when(col("days_since_last_order") <= 365, "Dormant")
                    .otherwise("Churned")) \
         .withColumn("_created_at", current_timestamp())
        
        # 寫入 Gold 層
        target_table = f"{self.catalog}.{self.gold_db}.dim_customers"
        dim_customers.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"    Created dim_customers with {dim_customers.count()} rows")
    
    def create_dim_products(self):
        """建立產品維度表"""
        print("🏗️ Creating dim_products...")
        
        products = self.spark.table(f"{self.catalog}.{self.silver_db}.cleaned_products")
        
        dim_products = products \
            .withColumn("_created_at", current_timestamp())
        
        target_table = f"{self.catalog}.{self.gold_db}.dim_products"
        dim_products.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"    Created dim_products with {dim_products.count()} rows")
    
    def create_fact_orders(self):
        """建立訂單事實表"""
        print("🏗️ Creating fact_orders...")
        
        orders = self.spark.table(f"{self.catalog}.{self.silver_db}.cleaned_orders")
        customers = self.spark.table(f"{self.catalog}.{self.gold_db}.dim_customers") \
            .select("customer_id", col("customer_id").alias("customer_key"))
        
        fact_orders = orders.join(
            customers,
            on="customer_id",
            how="left"
        ).withColumn("_created_at", current_timestamp())
        
        target_table = f"{self.catalog}.{self.gold_db}.fact_orders"
        fact_orders.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy("order_date") \
            .createOrReplace()
        
        print(f"    Created fact_orders with {fact_orders.count()} rows")
    
    def create_daily_sales_summary(self):
        """建立每日銷售彙總表"""
        print("🏗️ Creating daily_sales_summary...")
        
        orders = self.spark.table(f"{self.catalog}.{self.gold_db}.fact_orders")
        
        daily_summary = orders.groupBy("order_date").agg(
            count("*").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            sum("total_amount").alias("gross_revenue"),
            sum("subtotal").alias("net_revenue"),
            sum("tax").alias("total_tax"),
            sum("shipping_cost").alias("total_shipping"),
            avg("total_amount").alias("avg_order_value"),
            count(when(col("is_completed"), 1)).alias("completed_orders"),
            count(when(col("order_value_tier") == "High", 1)).alias("high_value_orders"),
            count(when(col("order_value_tier") == "Very High", 1)).alias("very_high_value_orders")
        ).withColumn("completion_rate",
                    col("completed_orders") / col("total_orders") * 100) \
         .withColumn("_created_at", current_timestamp())
        
        target_table = f"{self.catalog}.{self.gold_db}.daily_sales_summary"
        daily_summary.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"    Created daily_sales_summary with {daily_summary.count()} rows")
    
    def create_customer_rfm(self):
        """
        建立 RFM (Recency, Frequency, Monetary) 分析表
        用於客戶分群
        """
        print("🏗️ Creating customer_rfm...")
        
        customers = self.spark.table(f"{self.catalog}.{self.gold_db}.dim_customers")
        
        rfm = customers.select(
            "customer_id",
            "full_name",
            "customer_segment",
            "days_since_last_order",
            "lifetime_orders",
            "lifetime_revenue",
            # RFM Scores (1-5)
            when(col("days_since_last_order") <= 30, 5)
            .when(col("days_since_last_order") <= 60, 4)
            .when(col("days_since_last_order") <= 90, 3)
            .when(col("days_since_last_order") <= 180, 2)
            .otherwise(1).alias("recency_score"),
            
            when(col("lifetime_orders") >= 20, 5)
            .when(col("lifetime_orders") >= 10, 4)
            .when(col("lifetime_orders") >= 5, 3)
            .when(col("lifetime_orders") >= 2, 2)
            .otherwise(1).alias("frequency_score"),
            
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
                    .otherwise("Lost")) \
         .withColumn("_created_at", current_timestamp())
        
        target_table = f"{self.catalog}.{self.gold_db}.customer_rfm"
        rfm.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"    Created customer_rfm with {rfm.count()} rows")


def run_gold_aggregate():
    """執行 Gold 層聚合"""
    print("\n" + "=" * 60)
    print("GOLD LAYER AGGREGATION")
    print("=" * 60)
    
    spark = get_spark_iceberg_session("GoldAggregate")
    gold = GoldLayerAggregate(spark)
    
    gold.create_database()
    gold.create_dim_customers()
    gold.create_dim_products()
    gold.create_fact_orders()
    gold.create_daily_sales_summary()
    gold.create_customer_rfm()
    
    print("\n Gold layer aggregation complete!")
    spark.stop()


if __name__ == "__main__":
    run_gold_aggregate()
"""
Silver Layer - Data Cleaning and Transformation
清理和轉換資料，建立標準化的分析表格
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, lower, upper, coalesce, lit, when,
    current_timestamp, datediff, current_date,
    regexp_replace, concat_ws, round as spark_round
)
from pyspark.sql.types import DecimalType

from iceberg_config import get_spark_iceberg_session


class SilverLayerTransform:
    """Silver Layer: 清理和標準化資料"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.catalog = "nessie"
        self.bronze_db = "bronze"
        self.silver_db = "silver"
    
    def create_database(self):
        """建立 Silver 資料庫"""
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.silver_db}")
        print(f" Created database: {self.catalog}.{self.silver_db}")
    
    def transform_customers(self) -> DataFrame:
        """
        轉換客戶資料
        - 清理名稱
        - 標準化 email
        - 計算客戶年資
        """
        source_table = f"{self.catalog}.{self.bronze_db}.raw_customers"
        target_table = f"{self.catalog}.{self.silver_db}.cleaned_customers"
        
        print("🔄 Transforming customers...")
        
        # 讀取 Bronze 層資料
        raw_df = self.spark.table(source_table)
        
        # 轉換
        cleaned_df = raw_df \
            .withColumn("first_name", trim(col("first_name"))) \
            .withColumn("last_name", trim(col("last_name"))) \
            .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("customer_segment", 
                        coalesce(col("customer_segment"), lit("Unknown"))) \
            .withColumn("customer_tenure_days",
                        datediff(current_date(), col("registration_date"))) \
            .withColumn("is_active", lit(True)) \
            .withColumn("_transformed_at", current_timestamp()) \
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
                "customer_tenure_days",
                "is_active",
                "_transformed_at"
            )
        
        # 使用 MERGE 進行增量更新 (SCD Type 1)
        cleaned_df.createOrReplaceTempView("updates")
        
        self.spark.sql(f"""
            MERGE INTO {target_table} t
            USING updates s
            ON t.customer_id = s.customer_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        print(f"    Transformed {cleaned_df.count()} customers")
        return cleaned_df
    
    def transform_products(self) -> DataFrame:
        """
        轉換產品資料
        - 計算利潤率
        - 建立價格等級
        """
        source_table = f"{self.catalog}.{self.bronze_db}.raw_products"
        target_table = f"{self.catalog}.{self.silver_db}.cleaned_products"
        
        print("🔄 Transforming products...")
        
        raw_df = self.spark.table(source_table)
        
        cleaned_df = raw_df \
            .withColumn("product_name", trim(col("name"))) \
            .withColumn("category", trim(col("category"))) \
            .withColumn("brand", trim(col("brand"))) \
            .withColumn("current_price", col("price").cast(DecimalType(10, 2))) \
            .withColumn("cost", col("cost").cast(DecimalType(10, 2))) \
            .withColumn("profit_margin",
                        when(col("price") > 0,
                             spark_round((col("price") - col("cost")) / col("price") * 100, 2))
                        .otherwise(0)) \
            .withColumn("price_tier",
                        when(col("price") < 20, "Budget")
                        .when(col("price") < 50, "Standard")
                        .when(col("price") < 100, "Premium")
                        .otherwise("Luxury")) \
            .withColumn("is_in_stock", col("stock_quantity") > 0) \
            .withColumn("_transformed_at", current_timestamp()) \
            .select(
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "current_price",
                "cost",
                "profit_margin",
                "price_tier",
                "stock_quantity",
                "is_in_stock",
                "_transformed_at"
            )
        
        # 寫入 Silver 層
        cleaned_df.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f"    Transformed {cleaned_df.count()} products")
        return cleaned_df
    
    def transform_orders(self) -> DataFrame:
        """
        轉換訂單資料
        - 標準化狀態
        - 計算訂單價值等級
        """
        source_table = f"{self.catalog}.{self.bronze_db}.raw_orders"
        target_table = f"{self.catalog}.{self.silver_db}.cleaned_orders"
        
        print("🔄 Transforming orders...")
        
        raw_df = self.spark.table(source_table)
        
        cleaned_df = raw_df \
            .withColumn("order_status", lower(trim(col("status")))) \
            .withColumn("payment_method", lower(trim(col("payment_method")))) \
            .withColumn("order_date_key", 
                        col("order_date").cast("date").cast("string")
                        .substr(1, 10).cast("int")) \
            .withColumn("total_amount", col("total_amount").cast(DecimalType(12, 2))) \
            .withColumn("order_value_tier",
                        when(col("total_amount") < 50, "Very Low")
                        .when(col("total_amount") < 100, "Low")
                        .when(col("total_amount") < 200, "Medium")
                        .when(col("total_amount") < 500, "High")
                        .otherwise("Very High")) \
            .withColumn("is_completed", col("status") == "completed") \
            .withColumn("is_fulfilled", col("status").isin("shipped", "completed")) \
            .withColumn("_transformed_at", current_timestamp()) \
            .select(
                "order_id",
                "customer_id",
                "order_date",
                "order_date_key",
                "order_status",
                "payment_method",
                "subtotal",
                "tax",
                "shipping_cost",
                "total_amount",
                "order_value_tier",
                "is_completed",
                "is_fulfilled",
                "_transformed_at"
            )
        
        # 使用分區寫入
        cleaned_df.writeTo(target_table) \
            .tableProperty("format-version", "2") \
            .partitionedBy("order_date") \
            .createOrReplace()
        
        print(f"    Transformed {cleaned_df.count()} orders")
        return cleaned_df


def run_silver_transform():
    """執行 Silver 層轉換"""
    print("\n" + "=" * 60)
    print("SILVER LAYER TRANSFORMATION")
    print("=" * 60)
    
    spark = get_spark_iceberg_session("SilverTransform")
    silver = SilverLayerTransform(spark)
    
    silver.create_database()
    silver.transform_customers()
    silver.transform_products()
    silver.transform_orders()
    
    print("\n Silver layer transformation complete!")
    spark.stop()


if __name__ == "__main__":
    run_silver_transform()
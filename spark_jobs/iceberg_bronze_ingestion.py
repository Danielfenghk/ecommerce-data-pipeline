"""
Bronze Layer - Raw Data Ingestion to Iceberg
從來源系統載入原始資料到 Bronze 層
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from datetime import datetime

from iceberg_config import get_spark_iceberg_session


class BronzeLayerIngestion:
    """Bronze Layer: 原始資料層 - 完整保留來源資料"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.catalog = "nessie"
        self.database = "bronze"
    
    def create_database(self):
        """建立 Bronze 資料庫"""
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.database}")
        print(f"Created database: {self.catalog}.{self.database}")
    
    def ingest_customers(self, source_df: DataFrame) -> None:
        """
        載入客戶資料到 Bronze 層
        保留所有原始欄位，加入 metadata
        """
        table_name = f"{self.catalog}.{self.database}.raw_customers"
        
        # 加入 metadata 欄位
        df_with_metadata = source_df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("postgresql")) \
            .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # 寫入 Iceberg 表
        df_with_metadata.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .tableProperty("write.metadata.compression-codec", "gzip") \
            .createOrReplace()
        
        print(f" Ingested {df_with_metadata.count()} customers to {table_name}")
    
    def ingest_products(self, source_df: DataFrame) -> None:
        """載入產品資料到 Bronze 層"""
        table_name = f"{self.catalog}.{self.database}.raw_products"
        
        df_with_metadata = source_df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("postgresql")) \
            .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        df_with_metadata.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        
        print(f" Ingested {df_with_metadata.count()} products to {table_name}")
    
    def ingest_orders(self, source_df: DataFrame) -> None:
        """
        載入訂單資料到 Bronze 層
        使用分區以優化查詢效能
        """
        table_name = f"{self.catalog}.{self.database}.raw_orders"
        
        df_with_metadata = source_df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("postgresql")) \
            .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        # 使用 order_date 進行分區
        df_with_metadata.writeTo(table_name) \
            .tableProperty("format-version", "2") \
            .partitionedBy("order_date") \
            .createOrReplace()
        
        print(f" Ingested {df_with_metadata.count()} orders to {table_name}")
    
    def ingest_streaming_events(self, kafka_df: DataFrame) -> None:
        """
        從 Kafka 串流載入事件到 Bronze 層
        """
        table_name = f"{self.catalog}.{self.database}.raw_events"
        
        # Kafka 串流寫入
        kafka_df \
            .writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("path", table_name) \
            .option("checkpointLocation", "s3a://lakehouse/checkpoints/events") \
            .trigger(processingTime="1 minute") \
            .start()
        
        print(f" Started streaming ingestion to {table_name}")


def run_bronze_ingestion():
    """執行 Bronze 層資料載入"""
    print("\n" + "=" * 60)
    print("BRONZE LAYER INGESTION")
    print("=" * 60)
    
    spark = get_spark_iceberg_session("BronzeIngestion")
    bronze = BronzeLayerIngestion(spark)
    
    # 建立資料庫
    bronze.create_database()
    
    # 從 PostgreSQL 讀取資料
    jdbc_url = "jdbc:postgresql://postgres-source:5432/ecommerce_source"
    properties = {
        "user": "ecommerce_user",
        "password": "ecommerce_pass",
        "driver": "org.postgresql.Driver"
    }
    
    # 載入各表
    customers_df = spark.read.jdbc(jdbc_url, "customers", properties=properties)
    bronze.ingest_customers(customers_df)
    
    products_df = spark.read.jdbc(jdbc_url, "products", properties=properties)
    bronze.ingest_products(products_df)
    
    orders_df = spark.read.jdbc(jdbc_url, "orders", properties=properties)
    bronze.ingest_orders(orders_df)
    
    print("\n Bronze layer ingestion complete!")
    spark.stop()


if __name__ == "__main__":
    run_bronze_ingestion()
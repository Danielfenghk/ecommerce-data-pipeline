#!/usr/bin/env python3
"""
Spark Streaming Processing Job for Real-time Events
Processes clickstream and user events in real-time using Structured Streaming
"""

import os
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, sum as spark_sum, count, avg,
    min as spark_min, max as spark_max,
    date_format, to_date, to_timestamp, window,
    concat, concat_ws, upper, lower, trim,
    from_json, to_json, get_json_object, explode,
    current_timestamp, unix_timestamp, expr,
    collect_list, collect_set, first, last,
    round as spark_round, split, size
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, BooleanType, ArrayType, MapType
)


class StreamingEventsProcessor:
    """Real-time streaming processor for events using Spark Structured Streaming"""
    
    def __init__(self, spark: SparkSession = None):
        """Initialize the streaming processor"""
        self.spark = spark or self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for streaming"""
        return SparkSession.builder \
            .appName("StreamingEventsProcessing") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                    "org.postgresql:postgresql:42.5.0") \
            .getOrCreate()
    
    def get_clickstream_schema(self) -> StructType:
        """Define schema for clickstream events"""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("referrer_url", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("os", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category_id", StringType(), True),
            StructField("search_query", StringType(), True),
            StructField("cart_value", DoubleType(), True),
            StructField("properties", MapType(StringType(), StringType()), True)
        ])
    
    def get_order_event_schema(self) -> StructType:
        """Define schema for order events"""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("total_amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("items_count", IntegerType(), True)
        ])
    
    def read_from_kafka(self, topic: str, 
                        schema: StructType = None) -> DataFrame:
        """Read streaming data from Kafka"""
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON messages
        if schema:
            parsed_stream = raw_stream \
                .select(
                    col("key").cast(StringType()).alias("message_key"),
                    from_json(col("value").cast(StringType()), schema).alias("data"),
                    col("timestamp").alias("kafka_timestamp"),
                    col("partition"),
                    col("offset")
                ) \
                .select("message_key", "data.*", "kafka_timestamp", "partition", "offset")
        else:
            parsed_stream = raw_stream \
                .select(
                    col("key").cast(StringType()).alias("message_key"),
                    col("value").cast(StringType()).alias("raw_value"),
                    col("timestamp").alias("kafka_timestamp")
                )
        
        return parsed_stream
    
    def read_clickstream_events(self) -> DataFrame:
        """Read clickstream events from Kafka"""
        schema = self.get_clickstream_schema()
        return self.read_from_kafka("clickstream-events", schema)
    
    def read_order_events(self) -> DataFrame:
        """Read order events from Kafka"""
        schema = self.get_order_event_schema()
        return self.read_from_kafka("order-events", schema)
    
    def process_clickstream(self, stream_df: DataFrame) -> DataFrame:
        """Process clickstream events with enrichment"""
        processed = stream_df \
            .withColumn("event_date", to_date(col("timestamp"))) \
            .withColumn("event_hour", date_format(col("timestamp"), "HH").cast(IntegerType())) \
            .withColumn("event_minute", date_format(col("timestamp"), "mm").cast(IntegerType())) \
            .withColumn("is_mobile", 
                       col("device_type").isin(["mobile", "tablet"])) \
            .withColumn("is_logged_in", col("user_id").isNotNull()) \
            .withColumn("page_type",
                       when(col("page_url").contains("/product/"), "product")
                       .when(col("page_url").contains("/category/"), "category")
                       .when(col("page_url").contains("/cart"), "cart")
                       .when(col("page_url").contains("/checkout"), "checkout")
                       .when(col("page_url").contains("/search"), "search")
                       .when(col("page_url") == "/", "home")
                       .otherwise("other")) \
            .withColumn("has_referrer", col("referrer_url").isNotNull()) \
            .withColumn("is_organic",
                       ~col("referrer_url").contains("google") & 
                       ~col("referrer_url").contains("facebook") &
                       ~col("referrer_url").contains("utm_"))
        
        return processed
    
    def calculate_session_metrics(self, stream_df: DataFrame,
                                   watermark_duration: str = "30 minutes",
                                   window_duration: str = "5 minutes") -> DataFrame:
        """Calculate session-level metrics with windowing"""
        session_metrics = stream_df \
            .withWatermark("timestamp", watermark_duration) \
            .groupBy(
                window(col("timestamp"), window_duration),
                col("session_id")
            ) \
            .agg(
                count("event_id").alias("event_count"),
                countDistinct("page_url").alias("pages_viewed"),
                spark_sum(when(col("event_type") == "product_view", 1).otherwise(0))
                    .alias("products_viewed"),
                spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0))
                    .alias("add_to_cart_events"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0))
                    .alias("purchases"),
                first("user_id").alias("user_id"),
                first("device_type").alias("device_type"),
                first("country").alias("country"),
                spark_min("timestamp").alias("session_start"),
                spark_max("timestamp").alias("session_end"),
                spark_max("cart_value").alias("max_cart_value")
            ) \
            .withColumn("session_duration_seconds",
                       unix_timestamp(col("session_end")) - 
                       unix_timestamp(col("session_start"))) \
            .withColumn("is_converted", col("purchases") > 0) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        return session_metrics
    
    def calculate_realtime_kpis(self, stream_df: DataFrame,
                                 watermark_duration: str = "10 minutes",
                                 window_duration: str = "1 minute") -> DataFrame:
        """Calculate real-time KPIs with 1-minute windows"""
        kpis = stream_df \
            .withWatermark("timestamp", watermark_duration) \
            .groupBy(
                window(col("timestamp"), window_duration)
            ) \
            .agg(
                count("event_id").alias("total_events"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("active_sessions"),
                spark_sum(when(col("event_type") == "page_view", 1).otherwise(0))
                    .alias("page_views"),
                spark_sum(when(col("event_type") == "product_view", 1).otherwise(0))
                    .alias("product_views"),
                spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0))
                    .alias("add_to_carts"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0))
                    .alias("purchases"),
                spark_sum(when(col("is_mobile"), 1).otherwise(0)).alias("mobile_events"),
                countDistinct("country").alias("countries_active"),
                avg("cart_value").alias("avg_cart_value")
            ) \
            .withColumn("mobile_percentage",
                       spark_round(col("mobile_events") / col("total_events") * 100, 2)) \
            .withColumn("conversion_rate",
                       when(col("active_sessions") > 0,
                            spark_round(col("purchases") / col("active_sessions") * 100, 2))
                       .otherwise(0)) \
            .withColumn("cart_rate",
                       when(col("product_views") > 0,
                            spark_round(col("add_to_carts") / col("product_views") * 100, 2))
                       .otherwise(0)) \
            .withColumn("kpi_timestamp", current_timestamp()) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        return kpis
    
    def process_order_events(self, stream_df: DataFrame) -> DataFrame:
        """Process order events for real-time monitoring"""
        processed = stream_df \
            .withColumn("event_date", to_date(col("timestamp"))) \
            .withColumn("event_hour", date_format(col("timestamp"), "HH").cast(IntegerType())) \
            .withColumn("order_size_tier",
                       when(col("total_amount") < 50, "Small")
                       .when(col("total_amount") < 100, "Medium")
                       .when(col("total_amount") < 250, "Large")
                       .otherwise("Premium")) \
            .withColumn("is_high_value", col("total_amount") >= 200)
        
        return processed
    
    def calculate_order_metrics(self, stream_df: DataFrame,
                                 watermark_duration: str = "10 minutes",
                                 window_duration: str = "1 minute") -> DataFrame:
        """Calculate real-time order metrics"""
        order_metrics = stream_df \
            .withWatermark("timestamp", watermark_duration) \
            .groupBy(
                window(col("timestamp"), window_duration)
            ) \
            .agg(
                count("order_id").alias("orders_count"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                spark_sum("items_count").alias("total_items"),
                countDistinct("customer_id").alias("unique_customers"),
                spark_sum(when(col("status") == "completed", 1).otherwise(0))
                    .alias("completed_orders"),
                spark_sum(when(col("status") == "failed", 1).otherwise(0))
                    .alias("failed_orders"),
                spark_sum(when(col("is_high_value"), col("total_amount")).otherwise(0))
                    .alias("high_value_revenue"),
                collect_set("payment_method").alias("payment_methods_used")
            ) \
            .withColumn("completion_rate",
                       when(col("orders_count") > 0,
                            spark_round(col("completed_orders") / col("orders_count") * 100, 2))
                       .otherwise(0)) \
            .withColumn("high_value_percentage",
                       when(col("total_revenue") > 0,
                            spark_round(col("high_value_revenue") / col("total_revenue") * 100, 2))
                       .otherwise(0)) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        return order_metrics
    
    def detect_anomalies(self, stream_df: DataFrame,
                          metric_col: str,
                          threshold_multiplier: float = 2.0) -> DataFrame:
        """Detect anomalies in streaming metrics"""
        # Calculate rolling statistics
        anomalies = stream_df \
            .withColumn("is_anomaly",
                       col(metric_col) > (avg(col(metric_col)).over(
                           window(col("window_start"), "10 minutes")
                       ) * threshold_multiplier)) \
            .filter(col("is_anomaly"))
        
        return anomalies
    
    def write_to_kafka(self, stream_df: DataFrame, topic: str,
                       checkpoint_path: str) -> Any:
        """Write streaming data to Kafka"""
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        
        return stream_df \
            .select(
                to_json(struct("*")).alias("value")
            ) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .start()
    
    def write_to_console(self, stream_df: DataFrame,
                          output_mode: str = "append") -> Any:
        """Write streaming data to console for debugging"""
        return stream_df \
            .writeStream \
            .format("console") \
            .outputMode(output_mode) \
            .option("truncate", False) \
            .start()
    
    def write_to_postgres(self, stream_df: DataFrame, table_name: str,
                          checkpoint_path: str) -> Any:
        """Write streaming data to PostgreSQL using foreachBatch"""
        jdbc_url = os.getenv("WAREHOUSE_JDBC_URL",
                            "jdbc:postgresql://localhost:5432/warehouse_db")
        
        def write_batch(batch_df, batch_id):
            if batch_df.count() > 0:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", os.getenv("WAREHOUSE_USER", "postgres")) \
                    .option("password", os.getenv("WAREHOUSE_PASSWORD", "password")) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                print(f"Batch {batch_id}: Wrote {batch_df.count()} records to {table_name}")
        
        return stream_df \
            .writeStream \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .start()
    
    def write_to_data_lake(self, stream_df: DataFrame, path: str,
                           checkpoint_path: str,
                           partition_cols: list = None) -> Any:
        """Write streaming data to data lake in Parquet format"""
        writer = stream_df \
            .writeStream \
            .format("parquet") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        return writer.start()
    
    def run_clickstream_pipeline(self, output_console: bool = False):
        """Run the complete clickstream processing pipeline"""
        print("Starting clickstream streaming pipeline...")
        
        # Read from Kafka
        clickstream = self.read_clickstream_events()
        
        # Process events
        processed = self.process_clickstream(clickstream)
        
        # Calculate real-time KPIs
        kpis = self.calculate_realtime_kpis(processed)
        
        # Calculate session metrics
        sessions = self.calculate_session_metrics(processed)
        
        # Start streaming queries
        queries = []
        
        if output_console:
            # Debug output to console
            kpi_query = self.write_to_console(kpis, "complete")
            queries.append(kpi_query)
        else:
            # Write KPIs to Kafka
            kpi_query = self.write_to_kafka(
                kpis, 
                "realtime-kpis",
                "/tmp/checkpoints/kpis"
            )
            queries.append(kpi_query)
            
            # Write sessions to PostgreSQL
            session_query = self.write_to_postgres(
                sessions,
                "streaming_sessions",
                "/tmp/checkpoints/sessions"
            )
            queries.append(session_query)
            
            # Write processed events to data lake
            events_query = self.write_to_data_lake(
                processed,
                "s3a://data-lake/clickstream/",
                "/tmp/checkpoints/events",
                partition_cols=["event_date", "event_hour"]
            )
            queries.append(events_query)
        
        # Wait for termination
        for query in queries:
            query.awaitTermination()
    
    def run_orders_pipeline(self, output_console: bool = False):
        """Run the order events processing pipeline"""
        print("Starting order events streaming pipeline...")
        
        # Read from Kafka
        order_events = self.read_order_events()
        
        # Process events
        processed = self.process_order_events(order_events)
        
        # Calculate order metrics
        metrics = self.calculate_order_metrics(processed)
        
        # Start streaming queries
        queries = []
        
        if output_console:
            query = self.write_to_console(metrics, "complete")
            queries.append(query)
        else:
            # Write metrics to Kafka for monitoring
            metrics_query = self.write_to_kafka(
                metrics,
                "order-metrics",
                "/tmp/checkpoints/order-metrics"
            )
            queries.append(metrics_query)
            
            # Write to PostgreSQL
            db_query = self.write_to_postgres(
                metrics,
                "streaming_order_metrics",
                "/tmp/checkpoints/order-metrics-db"
            )
            queries.append(db_query)
        
        for query in queries:
            query.awaitTermination()
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """Main entry point for streaming processing job"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Streaming Events Processing Job')
    parser.add_argument('--pipeline', choices=['clickstream', 'orders', 'all'],
                       default='all', help='Pipeline to run')
    parser.add_argument('--console', action='store_true', 
                       help='Output to console for debugging')
    
    args = parser.parse_args()
    
    processor = StreamingEventsProcessor()
    
    try:
        if args.pipeline == 'clickstream':
            processor.run_clickstream_pipeline(output_console=args.console)
        elif args.pipeline == 'orders':
            processor.run_orders_pipeline(output_console=args.console)
        else:
            # Run both pipelines (would need threading for real use)
            processor.run_clickstream_pipeline(output_console=args.console)
    except Exception as e:
        print(f"Error during streaming processing: {e}")
        raise
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
"""
Bronze Layer
============

Raw data ingestion layer - preserves source data in its original form
with added metadata for lineage tracking.
"""

from typing import Optional, Dict, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, input_file_name

from .iceberg_manager import IcebergManager


class BronzeLayer:
    """
    Bronze Layer: Raw Data Ingestion
    
    Responsibilities:
    - Ingest data from various sources (JDBC, files, Kafka)
    - Preserve original data structure
    - Add ingestion metadata (_ingested_at, _source, _batch_id)
    - Store in Iceberg format with partitioning
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "nessie"):
        """
        Initialize Bronze Layer.
        
        Args:
            spark: SparkSession with Iceberg configured
            catalog: Iceberg catalog name
        """
        self.spark = spark
        self.catalog = catalog
        self.database = "bronze"
        self.manager = IcebergManager(spark, catalog)
        
        # Ensure database exists
        self.manager.create_database(self.database)
    
    def _add_metadata(self, 
                      df: DataFrame, 
                      source: str,
                      batch_id: Optional[str] = None) -> DataFrame:
        """
        Add ingestion metadata to DataFrame.
        
        Args:
            df: Source DataFrame
            source: Source system identifier
            batch_id: Optional batch identifier
        
        Returns:
            DataFrame with metadata columns
        """
        if batch_id is None:
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_system", lit(source)) \
            .withColumn("_batch_id", lit(batch_id))
    
    def ingest_from_jdbc(self,
                         jdbc_url: str,
                         source_table: str,
                         target_table: str,
                         connection_properties: Dict[str, str],
                         partition_column: Optional[str] = None,
                         num_partitions: int = 4,
                         fetch_size: int = 10000) -> int:
        """
        Ingest data from JDBC source.
        
        Args:
            jdbc_url: JDBC connection URL
            source_table: Source table name
            target_table: Target Iceberg table name
            connection_properties: JDBC connection properties
            partition_column: Column to use for parallel reads
            num_partitions: Number of parallel partitions
            fetch_size: JDBC fetch size
        
        Returns:
            Number of rows ingested
        """
        print(f"📥 Ingesting {source_table} from JDBC...")
        
        # Read from JDBC
        reader = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", source_table) \
            .option("fetchsize", fetch_size)
        
        # Add connection properties
        for key, value in connection_properties.items():
            reader = reader.option(key, value)
        
        # Add partitioning if specified
        if partition_column:
            reader = reader \
                .option("partitionColumn", partition_column) \
                .option("numPartitions", num_partitions)
        
        source_df = reader.load()
        
        # Add metadata
        df_with_metadata = self._add_metadata(source_df, "jdbc")
        
        # Write to Iceberg
        full_table_name = f"{self.catalog}.{self.database}.{target_table}"
        
        df_with_metadata.writeTo(full_table_name) \
            .tableProperty("format-version", "2") \
            .tableProperty("write.metadata.compression-codec", "gzip") \
            .createOrReplace()
        
        count = df_with_metadata.count()
        print(f"   ✅ Ingested {count} rows to {full_table_name}")
        
        return count
    
    def ingest_from_files(self,
                          path: str,
                          file_format: str,
                          target_table: str,
                          schema: Optional[str] = None,
                          options: Optional[Dict[str, str]] = None,
                          partition_by: Optional[List[str]] = None) -> int:
        """
        Ingest data from files (CSV, JSON, Parquet, etc.).
        
        Args:
            path: File path or pattern
            file_format: File format (csv, json, parquet, avro)
            target_table: Target Iceberg table name
            schema: Optional schema definition
            options: Reader options
            partition_by: Columns to partition by
        
        Returns:
            Number of rows ingested
        """
        print(f"📥 Ingesting files from {path}...")
        
        # Build reader
        reader = self.spark.read.format(file_format)
        
        if schema:
            reader = reader.schema(schema)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        source_df = reader.load(path)
        
        # Add file source info
        source_df = source_df.withColumn("_source_file", input_file_name())
        
        # Add metadata
        df_with_metadata = self._add_metadata(source_df, f"file:{file_format}")
        
        # Write to Iceberg
        full_table_name = f"{self.catalog}.{self.database}.{target_table}"
        
        writer = df_with_metadata.writeTo(full_table_name) \
            .tableProperty("format-version", "2")
        
        if partition_by:
            writer = writer.partitionedBy(*partition_by)
        
        writer.createOrReplace()
        
        count = df_with_metadata.count()
        print(f"   ✅ Ingested {count} rows to {full_table_name}")
        
        return count
    
    def ingest_from_dataframe(self,
                              df: DataFrame,
                              target_table: str,
                              source_name: str = "dataframe",
                              partition_by: Optional[List[str]] = None,
                              mode: str = "overwrite") -> int:
        """
        Ingest data from an existing DataFrame.
        
        Args:
            df: Source DataFrame
            target_table: Target Iceberg table name
            source_name: Source identifier for metadata
            partition_by: Columns to partition by
            mode: Write mode (overwrite or append)
        
        Returns:
            Number of rows ingested
        """
        print(f"📥 Ingesting DataFrame to {target_table}...")
        
        # Add metadata
        df_with_metadata = self._add_metadata(df, source_name)
        
        # Write to Iceberg
        full_table_name = f"{self.catalog}.{self.database}.{target_table}"
        
        writer = df_with_metadata.writeTo(full_table_name) \
            .tableProperty("format-version", "2")
        
        if partition_by:
            writer = writer.partitionedBy(*partition_by)
        
        if mode == "append":
            writer.append()
        else:
            writer.createOrReplace()
        
        count = df_with_metadata.count()
        print(f"   ✅ Ingested {count} rows to {full_table_name}")
        
        return count
    
    def ingest_from_kafka(self,
                          bootstrap_servers: str,
                          topic: str,
                          target_table: str,
                          starting_offsets: str = "earliest",
                          schema: Optional[str] = None) -> None:
        """
        Ingest data from Kafka (streaming).
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            target_table: Target Iceberg table name
            starting_offsets: Starting offset (earliest, latest)
            schema: Schema for value deserialization
        """
        print(f"📥 Starting Kafka ingestion from {topic}...")
        
        # Read from Kafka
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .load()
        
        # Parse value and add metadata
        from pyspark.sql.functions import from_json, col
        
        if schema:
            parsed_df = kafka_df \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
                .select(from_json(col("value"), schema).alias("data"), "timestamp") \
                .select("data.*", col("timestamp").alias("_kafka_timestamp"))
        else:
            parsed_df = kafka_df \
                .selectExpr("CAST(key AS STRING) as key", 
                           "CAST(value AS STRING) as value", 
                           "timestamp as _kafka_timestamp")
        
        # Add metadata
        final_df = parsed_df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_system", lit(f"kafka:{topic}"))
        
        # Write to Iceberg as streaming
        full_table_name = f"{self.catalog}.{self.database}.{target_table}"
        checkpoint_path = f"s3a://lakehouse/checkpoints/{target_table}"
        
        query = final_df.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("path", full_table_name) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime="1 minute") \
            .start()
        
        print(f"   ✅ Started streaming ingestion to {full_table_name}")
        
        return query
    
    def read_table(self, table_name: str) -> DataFrame:
        """Read a bronze table."""
        return self.manager.read_table(self.database, table_name)
    
    def list_tables(self) -> List[str]:
        """List all bronze tables."""
        return self.manager.list_tables(self.database)
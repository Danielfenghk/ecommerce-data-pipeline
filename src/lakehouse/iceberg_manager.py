"""
Iceberg Manager
===============

Manages Apache Iceberg catalog operations, table lifecycle,
and provides utilities for time travel and schema evolution.
"""

import os
from typing import Optional, List, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame


class IcebergManager:
    """
    Manages Iceberg catalog and table operations.
    
    Features:
    - Catalog management (Nessie)
    - Table creation and deletion
    - Schema evolution
    - Time travel queries
    - Snapshot management
    """
    
    def __init__(self, spark: SparkSession, catalog_name: str = "nessie"):
        """
        Initialize Iceberg Manager.
        
        Args:
            spark: SparkSession with Iceberg configured
            catalog_name: Name of the Iceberg catalog
        """
        self.spark = spark
        self.catalog = catalog_name
    
    # =========================================================================
    # DATABASE OPERATIONS
    # =========================================================================
    
    def create_database(self, database: str) -> None:
        """Create a database if it doesn't exist."""
        full_name = f"{self.catalog}.{database}"
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_name}")
        print(f"✅ Database created/verified: {full_name}")
    
    def drop_database(self, database: str, cascade: bool = False) -> None:
        """Drop a database."""
        full_name = f"{self.catalog}.{database}"
        cascade_sql = "CASCADE" if cascade else ""
        self.spark.sql(f"DROP DATABASE IF EXISTS {full_name} {cascade_sql}")
        print(f"🗑️ Database dropped: {full_name}")
    
    def list_databases(self) -> List[str]:
        """List all databases in the catalog."""
        result = self.spark.sql(f"SHOW DATABASES IN {self.catalog}").collect()
        return [row.namespace for row in result]
    
    def database_exists(self, database: str) -> bool:
        """Check if database exists."""
        return database in self.list_databases()
    
    # =========================================================================
    # TABLE OPERATIONS
    # =========================================================================
    
    def get_full_table_name(self, database: str, table: str) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{database}.{table}"
    
    def create_table(self, 
                     database: str, 
                     table: str, 
                     df: DataFrame,
                     partition_by: Optional[List[str]] = None,
                     properties: Optional[Dict[str, str]] = None) -> None:
        """
        Create an Iceberg table from a DataFrame.
        
        Args:
            database: Target database name
            table: Target table name
            df: DataFrame to create table from
            partition_by: List of columns to partition by
            properties: Table properties
        """
        full_name = self.get_full_table_name(database, table)
        
        # Start building the writer
        writer = df.writeTo(full_name)
        
        # Add table properties
        writer = writer.tableProperty("format-version", "2")
        writer = writer.tableProperty("write.metadata.compression-codec", "gzip")
        
        if properties:
            for key, value in properties.items():
                writer = writer.tableProperty(key, value)
        
        # Add partitioning
        if partition_by:
            writer = writer.partitionedBy(*partition_by)
        
        # Create or replace table
        writer.createOrReplace()
        print(f"✅ Table created: {full_name}")
    
    def drop_table(self, database: str, table: str) -> None:
        """Drop a table."""
        full_name = self.get_full_table_name(database, table)
        self.spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        print(f"🗑️ Table dropped: {full_name}")
    
    def list_tables(self, database: str) -> List[str]:
        """List all tables in a database."""
        result = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{database}").collect()
        return [row.tableName for row in result]
    
    def table_exists(self, database: str, table: str) -> bool:
        """Check if table exists."""
        try:
            tables = self.list_tables(database)
            return table in tables
        except:
            return False
    
    def read_table(self, database: str, table: str) -> DataFrame:
        """Read a table as DataFrame."""
        full_name = self.get_full_table_name(database, table)
        return self.spark.table(full_name)
    
    def describe_table(self, database: str, table: str) -> DataFrame:
        """Get table schema description."""
        full_name = self.get_full_table_name(database, table)
        return self.spark.sql(f"DESCRIBE {full_name}")
    
    # =========================================================================
    # TIME TRAVEL
    # =========================================================================
    
    def read_table_at_snapshot(self, 
                               database: str, 
                               table: str, 
                               snapshot_id: int) -> DataFrame:
        """
        Read table at a specific snapshot (time travel).
        
        Args:
            database: Database name
            table: Table name
            snapshot_id: Snapshot ID to read from
        
        Returns:
            DataFrame at the specified snapshot
        """
        full_name = self.get_full_table_name(database, table)
        return self.spark.sql(f"SELECT * FROM {full_name} VERSION AS OF {snapshot_id}")
    
    def read_table_at_timestamp(self, 
                                database: str, 
                                table: str, 
                                timestamp: str) -> DataFrame:
        """
        Read table at a specific timestamp.
        
        Args:
            database: Database name
            table: Table name
            timestamp: Timestamp string (e.g., '2024-01-15 10:00:00')
        
        Returns:
            DataFrame at the specified timestamp
        """
        full_name = self.get_full_table_name(database, table)
        return self.spark.sql(f"SELECT * FROM {full_name} TIMESTAMP AS OF '{timestamp}'")
    
    def get_table_history(self, database: str, table: str) -> DataFrame:
        """Get table history (all commits)."""
        full_name = self.get_full_table_name(database, table)
        return self.spark.sql(f"SELECT * FROM {full_name}.history")
    
    def get_table_snapshots(self, database: str, table: str) -> DataFrame:
        """Get all table snapshots."""
        full_name = self.get_full_table_name(database, table)
        return self.spark.sql(f"SELECT * FROM {full_name}.snapshots")
    
    def get_table_files(self, database: str, table: str) -> DataFrame:
        """Get data files for a table."""
        full_name = self.get_full_table_name(database, table)
        return self.spark.sql(f"SELECT * FROM {full_name}.files")
    
    def get_table_manifests(self, database: str, table: str) -> DataFrame:
        """Get manifest files for a table."""
        full_name = self.get_full_table_name(database, table)
        return self.spark.sql(f"SELECT * FROM {full_name}.manifests")
    
    # =========================================================================
    # SCHEMA EVOLUTION
    # =========================================================================
    
    def add_column(self, 
                   database: str, 
                   table: str, 
                   column_name: str, 
                   data_type: str,
                   comment: Optional[str] = None) -> None:
        """
        Add a new column to a table.
        
        Args:
            database: Database name
            table: Table name
            column_name: New column name
            data_type: Column data type
            comment: Optional column comment
        """
        full_name = self.get_full_table_name(database, table)
        comment_sql = f"COMMENT '{comment}'" if comment else ""
        self.spark.sql(f"ALTER TABLE {full_name} ADD COLUMN {column_name} {data_type} {comment_sql}")
        print(f"✅ Added column {column_name} to {full_name}")
    
    def rename_column(self, 
                      database: str, 
                      table: str, 
                      old_name: str, 
                      new_name: str) -> None:
        """Rename a column."""
        full_name = self.get_full_table_name(database, table)
        self.spark.sql(f"ALTER TABLE {full_name} RENAME COLUMN {old_name} TO {new_name}")
        print(f"✅ Renamed column {old_name} to {new_name}")
    
    def drop_column(self, database: str, table: str, column_name: str) -> None:
        """Drop a column from a table."""
        full_name = self.get_full_table_name(database, table)
        self.spark.sql(f"ALTER TABLE {full_name} DROP COLUMN {column_name}")
        print(f"✅ Dropped column {column_name}")
    
    # =========================================================================
    # PARTITION EVOLUTION
    # =========================================================================
    
    def add_partition_field(self, 
                            database: str, 
                            table: str, 
                            column: str,
                            transform: Optional[str] = None) -> None:
        """
        Add a partition field to a table.
        
        Args:
            database: Database name
            table: Table name
            column: Column to partition by
            transform: Optional transform (e.g., 'months', 'days', 'hours', 'bucket[N]')
        """
        full_name = self.get_full_table_name(database, table)
        
        if transform:
            partition_expr = f"{transform}({column})"
        else:
            partition_expr = column
        
        self.spark.sql(f"ALTER TABLE {full_name} ADD PARTITION FIELD {partition_expr}")
        print(f"✅ Added partition field: {partition_expr}")
    
    def drop_partition_field(self, database: str, table: str, column: str) -> None:
        """Drop a partition field."""
        full_name = self.get_full_table_name(database, table)
        self.spark.sql(f"ALTER TABLE {full_name} DROP PARTITION FIELD {column}")
        print(f"✅ Dropped partition field: {column}")
    
    # =========================================================================
    # MAINTENANCE OPERATIONS
    # =========================================================================
    
    def expire_snapshots(self, 
                         database: str, 
                         table: str, 
                         older_than: str) -> None:
        """
        Expire old snapshots to reclaim storage.
        
        Args:
            database: Database name
            table: Table name
            older_than: Timestamp string (e.g., '2024-01-01 00:00:00')
        """
        full_name = self.get_full_table_name(database, table)
        self.spark.sql(f"""
            CALL {self.catalog}.system.expire_snapshots(
                table => '{full_name}',
                older_than => TIMESTAMP '{older_than}'
            )
        """)
        print(f"✅ Expired snapshots older than {older_than}")
    
    def remove_orphan_files(self, database: str, table: str) -> None:
        """Remove orphan files that are no longer referenced."""
        full_name = self.get_full_table_name(database, table)
        self.spark.sql(f"""
            CALL {self.catalog}.system.remove_orphan_files(
                table => '{full_name}'
            )
        """)
        print(f"✅ Removed orphan files from {full_name}")
    
    def rewrite_data_files(self, database: str, table: str) -> None:
        """Compact data files for better query performance."""
        full_name = self.get_full_table_name(database, table)
        self.spark.sql(f"""
            CALL {self.catalog}.system.rewrite_data_files(
                table => '{full_name}'
            )
        """)
        print(f"✅ Compacted data files in {full_name}")
    
    # =========================================================================
    # STATISTICS
    # =========================================================================
    
    def get_table_stats(self, database: str, table: str) -> Dict[str, Any]:
        """Get comprehensive table statistics."""
        full_name = self.get_full_table_name(database, table)
        
        # Row count
        row_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {full_name}").first()['cnt']
        
        # Snapshot count
        snapshots = self.spark.sql(f"SELECT * FROM {full_name}.snapshots").count()
        
        # File count and size
        files_df = self.spark.sql(f"SELECT * FROM {full_name}.files")
        file_count = files_df.count()
        
        try:
            total_size = files_df.agg({"file_size_in_bytes": "sum"}).first()[0] or 0
        except:
            total_size = 0
        
        return {
            'table': full_name,
            'row_count': row_count,
            'snapshot_count': snapshots,
            'file_count': file_count,
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }
    
    def print_table_info(self, database: str, table: str) -> None:
        """Print detailed table information."""
        full_name = self.get_full_table_name(database, table)
        
        print(f"\n📊 Table Info: {full_name}")
        print("-" * 50)
        
        # Schema
        print("\n📋 Schema:")
        self.describe_table(database, table).show(truncate=False)
        
        # Stats
        stats = self.get_table_stats(database, table)
        print(f"\n📈 Statistics:")
        print(f"   Rows: {stats['row_count']:,}")
        print(f"   Snapshots: {stats['snapshot_count']}")
        print(f"   Files: {stats['file_count']}")
        print(f"   Size: {stats['total_size_mb']} MB")
        
        # Recent history
        print("\n📜 Recent History:")
        self.get_table_history(database, table).orderBy("made_current_at", ascending=False).limit(5).show()
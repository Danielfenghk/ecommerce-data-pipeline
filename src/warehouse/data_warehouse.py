"""
Data Warehouse Module
Manages data warehouse operations, loading, and querying
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch, execute_values
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime
from contextlib import contextmanager
import json

from src.config.settings import Settings

logger = logging.getLogger(__name__)


class DataWarehouse:
    """Main data warehouse class for managing DW operations"""
    
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.connection_params = {
            'host': self.settings.DW_HOST,
            'port': self.settings.DW_PORT,
            'database': self.settings.DW_DATABASE,
            'user': self.settings.DW_USER,
            'password': self.settings.DW_PASSWORD
        }
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """Get database connection with context manager"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> None:
        """Execute a query without returning results"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
                logger.debug(f"Query executed successfully")
    
    def fetch_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame"""
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[tuple]:
        """Execute a query and return single result"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()
    
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """Check if table exists"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """
        result = self.fetch_one(query, (schema, table_name))
        return result[0] if result else False
    
    def get_table_row_count(self, table_name: str, schema: str = 'public') -> int:
        """Get row count for a table"""
        query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                return result[0] if result else 0
    
    def get_table_columns(self, table_name: str, schema: str = 'public') -> List[Dict]:
        """Get column information for a table"""
        query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        df = self.fetch_query(query, (schema, table_name))
        return df.to_dict('records')
    
    def create_schema(self, schema_name: str) -> None:
        """Create a schema if it doesn't exist"""
        query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema_name)
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        logger.info(f"Schema '{schema_name}' created or already exists")
    
    def drop_table(self, table_name: str, schema: str = 'public', cascade: bool = False) -> None:
        """Drop a table"""
        cascade_str = "CASCADE" if cascade else ""
        query = sql.SQL("DROP TABLE IF EXISTS {}.{} {}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.SQL(cascade_str)
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        logger.info(f"Table '{schema}.{table_name}' dropped")
    
    def truncate_table(self, table_name: str, schema: str = 'public') -> None:
        """Truncate a table"""
        query = sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY").format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        logger.info(f"Table '{schema}.{table_name}' truncated")
    
    def vacuum_table(self, table_name: str, schema: str = 'public', analyze: bool = True) -> None:
        """Vacuum a table for maintenance"""
        analyze_str = "ANALYZE" if analyze else ""
        query = f"VACUUM {analyze_str} {schema}.{table_name}"
        
        with self.get_connection() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(query)
        logger.info(f"Table '{schema}.{table_name}' vacuumed")
    
    def get_warehouse_stats(self) -> Dict[str, Any]:
        """Get warehouse statistics"""
        stats = {}
        
        # Get database size
        size_query = """
            SELECT pg_size_pretty(pg_database_size(current_database()))
        """
        result = self.fetch_one(size_query)
        stats['database_size'] = result[0] if result else 'Unknown'
        
        # Get table count
        table_count_query = """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """
        result = self.fetch_one(table_count_query)
        stats['table_count'] = result[0] if result else 0
        
        # Get total rows across all tables
        tables_query = """
            SELECT schemaname, tablename 
            FROM pg_tables 
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """
        tables_df = self.fetch_query(tables_query)
        
        total_rows = 0
        table_sizes = []
        for _, row in tables_df.iterrows():
            count = self.get_table_row_count(row['tablename'], row['schemaname'])
            total_rows += count
            table_sizes.append({
                'schema': row['schemaname'],
                'table': row['tablename'],
                'row_count': count
            })
        
        stats['total_rows'] = total_rows
        stats['tables'] = table_sizes
        
        return stats


class WarehouseLoader:
    """Class for loading data into the warehouse"""
    
    def __init__(self, warehouse: DataWarehouse):
        self.warehouse = warehouse
        self.load_history = []
    
    def load_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'dim',
        key_column: str = None,
        scd_type: int = 1,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Load data into a dimension table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Target schema
            key_column: Primary key column
            scd_type: Slowly Changing Dimension type (1 or 2)
            batch_size: Batch size for inserts
            
        Returns:
            Load statistics
        """
        logger.info(f"Loading dimension table: {schema}.{table_name}")
        start_time = datetime.now()
        
        # Add metadata columns
        df['etl_loaded_at'] = datetime.now()
        df['etl_updated_at'] = datetime.now()
        
        if scd_type == 2:
            df['effective_from'] = datetime.now()
            df['effective_to'] = pd.Timestamp('9999-12-31')
            df['is_current'] = True
        
        # Determine insert strategy
        if key_column and self.warehouse.table_exists(table_name, schema):
            if scd_type == 1:
                result = self._upsert_scd1(df, table_name, schema, key_column, batch_size)
            else:
                result = self._upsert_scd2(df, table_name, schema, key_column, batch_size)
        else:
            result = self._bulk_insert(df, table_name, schema, batch_size)
        
        end_time = datetime.now()
        load_stats = {
            'table': f"{schema}.{table_name}",
            'records_processed': len(df),
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': (end_time - start_time).total_seconds(),
            **result
        }
        
        self.load_history.append(load_stats)
        logger.info(f"Dimension load complete: {load_stats}")
        
        return load_stats
    
    def load_fact(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'fact',
        partition_column: str = None,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Load data into a fact table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Target schema
            partition_column: Column used for partitioning
            batch_size: Batch size for inserts
            
        Returns:
            Load statistics
        """
        logger.info(f"Loading fact table: {schema}.{table_name}")
        start_time = datetime.now()
        
        # Add metadata columns
        df['etl_loaded_at'] = datetime.now()
        
        # If partitioned, ensure data is sorted
        if partition_column and partition_column in df.columns:
            df = df.sort_values(partition_column)
        
        result = self._bulk_insert(df, table_name, schema, batch_size)
        
        end_time = datetime.now()
        load_stats = {
            'table': f"{schema}.{table_name}",
            'records_processed': len(df),
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': (end_time - start_time).total_seconds(),
            **result
        }
        
        self.load_history.append(load_stats)
        logger.info(f"Fact load complete: {load_stats}")
        
        return load_stats
    
    def _bulk_insert(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        batch_size: int
    ) -> Dict[str, Any]:
        """Perform bulk insert"""
        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]
        
        insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        records_inserted = 0
        with self.warehouse.get_connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    execute_values(cur, insert_query, batch, page_size=batch_size)
                    records_inserted += len(batch)
                    logger.debug(f"Inserted batch: {records_inserted}/{len(values)}")
                conn.commit()
        
        return {'records_inserted': records_inserted, 'records_updated': 0}
    
    def _upsert_scd1(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        key_column: str,
        batch_size: int
    ) -> Dict[str, Any]:
        """Perform SCD Type 1 upsert (overwrite)"""
        columns = df.columns.tolist()
        update_columns = [c for c in columns if c != key_column]
        
        # Create upsert query
        insert_cols = sql.SQL(', ').join(map(sql.Identifier, columns))
        update_set = sql.SQL(', ').join([
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_columns
        ])
        
        upsert_query = sql.SQL("""
            INSERT INTO {}.{} ({})
            VALUES %s
            ON CONFLICT ({})
            DO UPDATE SET {}
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            insert_cols,
            sql.Identifier(key_column),
            update_set
        )
        
        values = [tuple(row) for row in df.values]
        
        with self.warehouse.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, upsert_query, values, page_size=batch_size)
                conn.commit()
        
        return {'records_inserted': len(values), 'records_updated': 0}
    
    def _upsert_scd2(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        key_column: str,
        batch_size: int
    ) -> Dict[str, Any]:
        """Perform SCD Type 2 upsert (historical tracking)"""
        records_inserted = 0
        records_updated = 0
        
        with self.warehouse.get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    key_value = row[key_column]
                    
                    # Check if record exists
                    check_query = sql.SQL("""
                        SELECT * FROM {}.{}
                        WHERE {} = %s AND is_current = true
                    """).format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.Identifier(key_column)
                    )
                    cur.execute(check_query, (key_value,))
                    existing = cur.fetchone()
                    
                    if existing:
                        # Close existing record
                        update_query = sql.SQL("""
                            UPDATE {}.{}
                            SET is_current = false, effective_to = %s
                            WHERE {} = %s AND is_current = true
                        """).format(
                            sql.Identifier(schema),
                            sql.Identifier(table_name),
                            sql.Identifier(key_column)
                        )
                        cur.execute(update_query, (datetime.now(), key_value))
                        records_updated += 1
                    
                    # Insert new record
                    columns = df.columns.tolist()
                    values = tuple(row.values)
                    insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(map(sql.Identifier, columns)),
                        sql.SQL(', ').join([sql.Placeholder()] * len(columns))
                    )
                    cur.execute(insert_query, values)
                    records_inserted += 1
                
                conn.commit()
        
        return {'records_inserted': records_inserted, 'records_updated': records_updated}
    
    def refresh_materialized_view(self, view_name: str, schema: str = 'public', concurrent: bool = True) -> None:
        """Refresh a materialized view"""
        concurrent_str = "CONCURRENTLY" if concurrent else ""
        query = f"REFRESH MATERIALIZED VIEW {concurrent_str} {schema}.{view_name}"
        
        with self.warehouse.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        
        logger.info(f"Materialized view '{schema}.{view_name}' refreshed")
    
    def execute_stored_procedure(self, procedure_name: str, schema: str = 'public', params: tuple = None) -> None:
        """Execute a stored procedure"""
        if params:
            placeholders = ', '.join(['%s'] * len(params))
            query = f"CALL {schema}.{procedure_name}({placeholders})"
        else:
            query = f"CALL {schema}.{procedure_name}()"
        
        with self.warehouse.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
        
        logger.info(f"Stored procedure '{schema}.{procedure_name}' executed")
    
    def get_load_history(self) -> pd.DataFrame:
        """Get load history as DataFrame"""
        return pd.DataFrame(self.load_history)


class IncrementalLoader:
    """Class for incremental data loading"""
    
    def __init__(self, warehouse: DataWarehouse):
        self.warehouse = warehouse
        self._ensure_watermark_table()
    
    def _ensure_watermark_table(self) -> None:
        """Ensure watermark tracking table exists"""
        query = """
            CREATE TABLE IF NOT EXISTS etl.load_watermarks (
                table_name VARCHAR(255) PRIMARY KEY,
                last_loaded_value VARCHAR(255),
                last_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count BIGINT,
                status VARCHAR(50)
            )
        """
        self.warehouse.create_schema('etl')
        self.warehouse.execute_query(query)
    
    def get_watermark(self, table_name: str) -> Optional[str]:
        """Get the last loaded watermark value"""
        query = """
            SELECT last_loaded_value FROM etl.load_watermarks
            WHERE table_name = %s
        """
        result = self.warehouse.fetch_one(query, (table_name,))
        return result[0] if result else None
    
    def update_watermark(self, table_name: str, watermark_value: str, row_count: int) -> None:
        """Update the watermark after successful load"""
        query = """
            INSERT INTO etl.load_watermarks (table_name, last_loaded_value, last_loaded_at, row_count, status)
            VALUES (%s, %s, CURRENT_TIMESTAMP, %s, 'success')
            ON CONFLICT (table_name)
            DO UPDATE SET 
                last_loaded_value = EXCLUDED.last_loaded_value,
                last_loaded_at = EXCLUDED.last_loaded_at,
                row_count = EXCLUDED.row_count,
                status = EXCLUDED.status
        """
        self.warehouse.execute_query(query, (table_name, watermark_value, row_count))
        logger.info(f"Watermark updated for {table_name}: {watermark_value}")
    
    def load_incremental(
        self,
        source_df: pd.DataFrame,
        table_name: str,
        watermark_column: str,
        schema: str = 'public'
    ) -> Dict[str, Any]:
        """
        Load data incrementally based on watermark
        
        Args:
            source_df: Source DataFrame
            table_name: Target table name
            watermark_column: Column to use as watermark
            schema: Target schema
            
        Returns:
            Load statistics
        """
        # Get current watermark
        current_watermark = self.get_watermark(f"{schema}.{table_name}")
        
        # Filter new data
        if current_watermark and watermark_column in source_df.columns:
            source_df = source_df[source_df[watermark_column] > current_watermark]
        
        if len(source_df) == 0:
            logger.info(f"No new data to load for {schema}.{table_name}")
            return {'records_loaded': 0, 'status': 'no_new_data'}
        
        # Get new watermark value
        new_watermark = str(source_df[watermark_column].max())
        
        # Load data
        loader = WarehouseLoader(self.warehouse)
        result = loader.load_fact(source_df, table_name, schema)
        
        # Update watermark
        self.update_watermark(f"{schema}.{table_name}", new_watermark, len(source_df))
        
        return {
            'records_loaded': len(source_df),
            'previous_watermark': current_watermark,
            'new_watermark': new_watermark,
            'status': 'success'
        }


if __name__ == "__main__":
    # Example usage
    from src.config.settings import Settings
    
    settings = Settings()
    warehouse = DataWarehouse(settings)
    
    # Get warehouse stats
    stats = warehouse.get_warehouse_stats()
    print("\nWarehouse Statistics:")
    print(json.dumps(stats, indent=2, default=str))
    
    # Example dimension load
    sample_dim = pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003'],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Wilson'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com']
    })
    
    loader = WarehouseLoader(warehouse)
    # result = loader.load_dimension(sample_dim, 'dim_customer', 'dim', 'customer_id')
    # print(f"\nLoad result: {result}")
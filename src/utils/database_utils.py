"""
Database utility functions for the E-Commerce Data Pipeline.
Provides connection management, query execution, and data loading utilities.
"""

import pandas as pd
from typing import Optional, List, Dict, Any, Generator
from contextlib import contextmanager
import psycopg2
from psycopg2 import sql, extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings, DatabaseConfig
from src.utils.logging_utils import logger, log_execution_time


class DatabaseConnection:
    """Database connection manager with connection pooling."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._engine: Optional[Engine] = None
        self._session_factory = None
    
    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.config.connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
        return self._engine
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager for database session."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_connection(self):
        """Context manager for raw psycopg2 connection."""
        conn = psycopg2.connect(**self.config.to_dict())
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            raise
    
    def close(self):
        """Close all connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


class QueryExecutor:
    """Execute database queries with logging and error handling."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    @log_execution_time
    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        fetch: bool = False
    ) -> Optional[List[tuple]]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True, else None
        """
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or {})
                if fetch:
                    return cur.fetchall()
                return None
    
    @log_execution_time
    def execute_many(
        self,
        query: str,
        data: List[tuple],
        batch_size: int = 1000
    ) -> int:
        """
        Execute query for multiple records with batching.
        
        Args:
            query: SQL query string
            data: List of data tuples
            batch_size: Batch size for inserts
            
        Returns:
            Number of affected rows
        """
        total_affected = 0
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    extras.execute_batch(cur, query, batch)
                    total_affected += len(batch)
                    logger.debug(f"Processed batch {i // batch_size + 1}")
        
        return total_affected
    
    @log_execution_time
    def read_to_dataframe(
        self,
        query: str,
        params: Optional[Dict] = None,
        chunksize: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read query results into pandas DataFrame.
        
        Args:
            query: SQL query string
            params: Query parameters
            chunksize: If provided, return iterator of DataFrames
            
        Returns:
            DataFrame with query results
        """
        return pd.read_sql(
            query,
            self.db.engine,
            params=params,
            chunksize=chunksize
        )
    
    @log_execution_time
    def read_table(
        self,
        table_name: str,
        schema: Optional[str] = None,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Read entire table into DataFrame.
        
        Args:
            table_name: Table name
            schema: Schema name
            columns: List of columns to select
            where: WHERE clause condition
            
        Returns:
            DataFrame with table data
        """
        cols = ', '.join(columns) if columns else '*'
        schema_prefix = f"{schema}." if schema else ""
        query = f"SELECT {cols} FROM {schema_prefix}{table_name}"
        
        if where:
            query += f" WHERE {where}"
        
        return self.read_to_dataframe(query)


class DataLoader:
    """Load data into database tables."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.executor = QueryExecutor(db_connection)
    
    @log_execution_time
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        if_exists: str = 'append',
        index: bool = False,
        chunksize: int = 10000
    ) -> int:
        """
        Load DataFrame into database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Target schema
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to write index
            chunksize: Number of rows per batch
            
        Returns:
            Number of rows loaded
        """
        df.to_sql(
            table_name,
            self.db.engine,
            schema=schema,
            if_exists=if_exists,
            index=index,
            chunksize=chunksize,
            method='multi'
        )
        return len(df)
    
    @log_execution_time
    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: List[str],
        schema: Optional[str] = None
    ) -> int:
        """
        Upsert DataFrame into database table.
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            key_columns: Columns to use for conflict detection
            schema: Target schema
            
        Returns:
            Number of rows processed
        """
        if df.empty:
            return 0
        
        columns = list(df.columns)
        schema_prefix = f"{schema}." if schema else ""
        full_table = f"{schema_prefix}{table_name}"
        
        # Build upsert query
        insert_cols = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        update_cols = ', '.join(
            f"{col} = EXCLUDED.{col}" 
            for col in columns if col not in key_columns
        )
        conflict_cols = ', '.join(key_columns)
        
        query = f"""
            INSERT INTO {full_table} ({insert_cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_cols}
        """
        
        data = [tuple(row) for row in df.values]
        return self.executor.execute_many(query, data)
    
    @log_execution_time
    def bulk_insert(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        schema: Optional[str] = None
    ) -> int:
        """
        Bulk insert dictionaries into table.
        
        Args:
            data: List of dictionaries to insert
            table_name: Target table name
            schema: Target schema
            
        Returns:
            Number of rows inserted
        """
        if not data:
            return 0
        
        df = pd.DataFrame(data)
        return self.load_dataframe(df, table_name, schema)
    
    @log_execution_time
    def copy_from_csv(
        self,
        file_path: str,
        table_name: str,
        schema: Optional[str] = None,
        delimiter: str = ',',
        null_string: str = ''
    ) -> int:
        """
        Copy data from CSV file using COPY command (faster than INSERT).
        
        Args:
            file_path: Path to CSV file
            table_name: Target table name
            schema: Target schema
            delimiter: Field delimiter
            null_string: String representing NULL
            
        Returns:
            Number of rows copied
        """
        schema_prefix = f"{schema}." if schema else ""
        full_table = f"{schema_prefix}{table_name}"
        
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                with open(file_path, 'r') as f:
                    cur.copy_expert(
                        f"COPY {full_table} FROM STDIN WITH CSV HEADER DELIMITER '{delimiter}' NULL '{null_string}'",
                        f
                    )
                return cur.rowcount


# Pre-configured database connections
source_db = DatabaseConnection(settings.source_db)
warehouse_db = DatabaseConnection(settings.warehouse_db)

# Pre-configured executors
source_executor = QueryExecutor(source_db)
warehouse_executor = QueryExecutor(warehouse_db)

# Pre-configured loaders
source_loader = DataLoader(source_db)
warehouse_loader = DataLoader(warehouse_db)


__all__ = [
    'DatabaseConnection', 'QueryExecutor', 'DataLoader',
    'source_db', 'warehouse_db',
    'source_executor', 'warehouse_executor',
    'source_loader', 'warehouse_loader'
]
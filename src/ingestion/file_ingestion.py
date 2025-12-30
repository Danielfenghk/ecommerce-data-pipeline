"""
File-based data ingestion module.
Handles reading data from CSV, JSON, Parquet, and Excel files.
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Generator, Union
from datetime import datetime
import json
import glob

from src.config import settings
from src.utils.logging_utils import logger, log_execution_time


class FileIngestion:
    """Base class for file-based data ingestion."""
    
    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or settings.data_path
    
    def _resolve_path(self, file_path: Union[str, Path]) -> Path:
        """Resolve file path relative to base path."""
        path = Path(file_path)
        if not path.is_absolute():
            path = self.base_path / path
        return path
    
    def _validate_file(self, file_path: Path) -> None:
        """Validate that file exists."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")


class CSVIngestion(FileIngestion):
    """Ingestion handler for CSV files."""
    
    @log_execution_time
    def read_csv(
        self,
        file_path: Union[str, Path],
        delimiter: str = ',',
        encoding: str = 'utf-8',
        date_columns: Optional[List[str]] = None,
        dtype: Optional[Dict[str, Any]] = None,
        usecols: Optional[List[str]] = None,
        chunksize: Optional[int] = None,
        **kwargs
    ) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Read CSV file into DataFrame.
        
        Args:
            file_path: Path to CSV file
            delimiter: Column delimiter
            encoding: File encoding
            date_columns: Columns to parse as dates
            dtype: Column data types
            usecols: Columns to read
            chunksize: If specified, return iterator of chunks
            **kwargs: Additional pandas.read_csv arguments
            
        Returns:
            DataFrame or DataFrame iterator if chunksize specified
        """
        path = self._resolve_path(file_path)
        self._validate_file(path)
        
        logger.info(f"Reading CSV file: {path}")
        
        parse_dates = date_columns or []
        
        return pd.read_csv(
            path,
            delimiter=delimiter,
            encoding=encoding,
            parse_dates=parse_dates,
            dtype=dtype,
            usecols=usecols,
            chunksize=chunksize,
            **kwargs
        )
    
    @log_execution_time
    def read_csv_directory(
        self,
        directory: Union[str, Path],
        pattern: str = '*.csv',
        **kwargs
    ) -> pd.DataFrame:
        """
        Read all CSV files in directory into single DataFrame.
        
        Args:
            directory: Directory path
            pattern: Glob pattern for files
            **kwargs: Arguments passed to read_csv
            
        Returns:
            Combined DataFrame
        """
        dir_path = self._resolve_path(directory)
        
        if not dir_path.is_dir():
            raise NotADirectoryError(f"Not a directory: {dir_path}")
        
        files = list(dir_path.glob(pattern))
        logger.info(f"Found {len(files)} CSV files in {dir_path}")
        
        if not files:
            return pd.DataFrame()
        
        dfs = []
        for file in files:
            df = self.read_csv(file, **kwargs)
            df['_source_file'] = file.name
            dfs.append(df)
        
        return pd.concat(dfs, ignore_index=True)
    
    @log_execution_time
    def write_csv(
        self,
        df: pd.DataFrame,
        file_path: Union[str, Path],
        index: bool = False,
        **kwargs
    ) -> None:
        """
        Write DataFrame to CSV file.
        
        Args:
            df: DataFrame to write
            file_path: Output file path
            index: Whether to include index
            **kwargs: Additional pandas.to_csv arguments
        """
        path = self._resolve_path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing {len(df)} rows to CSV: {path}")
        df.to_csv(path, index=index, **kwargs)


class JSONIngestion(FileIngestion):
    """Ingestion handler for JSON files."""
    
    @log_execution_time
    def read_json(
        self,
        file_path: Union[str, Path],
        orient: str = 'records',
        lines: bool = False,
        **kwargs
    ) -> Union[pd.DataFrame, Dict, List]:
        """
        Read JSON file.
        
        Args:
            file_path: Path to JSON file
            orient: JSON orientation for DataFrame
            lines: Whether file contains JSON lines
            **kwargs: Additional arguments
            
        Returns:
            DataFrame or parsed JSON
        """
        path = self._resolve_path(file_path)
        self._validate_file(path)
        
        logger.info(f"Reading JSON file: {path}")
        
        if lines:
            return pd.read_json(path, orient=orient, lines=True, **kwargs)
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict) and 'data' in data:
            return pd.DataFrame(data['data'])
        
        return data
    
    @log_execution_time
    def read_json_lines(
        self,
        file_path: Union[str, Path],
        chunksize: Optional[int] = None
    ) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Read JSON Lines file.
        
        Args:
            file_path: Path to JSON Lines file
            chunksize: If specified, return iterator of chunks
            
        Returns:
            DataFrame or DataFrame iterator
        """
        path = self._resolve_path(file_path)
        self._validate_file(path)
        
        logger.info(f"Reading JSON Lines file: {path}")
        
        return pd.read_json(path, lines=True, chunksize=chunksize)
    
    @log_execution_time
    def write_json(
        self,
        data: Union[pd.DataFrame, Dict, List],
        file_path: Union[str, Path],
        orient: str = 'records',
        lines: bool = False,
        indent: int = 2,
        **kwargs
    ) -> None:
        """
        Write data to JSON file.
        
        Args:
            data: Data to write
            file_path: Output file path
            orient: JSON orientation
            lines: Whether to write JSON lines
            indent: JSON indentation
            **kwargs: Additional arguments
        """
        path = self._resolve_path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing to JSON: {path}")
        
        if isinstance(data, pd.DataFrame):
            data.to_json(path, orient=orient, lines=lines, indent=indent, **kwargs)
        else:
            with open(path, 'w') as f:
                json.dump(data, f, indent=indent, default=str)


class ParquetIngestion(FileIngestion):
    """Ingestion handler for Parquet files."""
    
    @log_execution_time
    def read_parquet(
        self,
        file_path: Union[str, Path],
        columns: Optional[List[str]] = None,
        filters: Optional[List[tuple]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Read Parquet file.
        
        Args:
            file_path: Path to Parquet file
            columns: Columns to read
            filters: Row group filters
            **kwargs: Additional arguments
            
        Returns:
            DataFrame
        """
        path = self._resolve_path(file_path)
        self._validate_file(path)
        
        logger.info(f"Reading Parquet file: {path}")
        
        return pd.read_parquet(
            path,
            columns=columns,
            filters=filters,
            **kwargs
        )
    
    @log_execution_time
    def read_parquet_directory(
        self,
        directory: Union[str, Path],
        **kwargs
    ) -> pd.DataFrame:
        """
        Read partitioned Parquet dataset.
        
        Args:
            directory: Directory path
            **kwargs: Arguments passed to read_parquet
            
        Returns:
            Combined DataFrame
        """
        dir_path = self._resolve_path(directory)
        
        logger.info(f"Reading Parquet directory: {dir_path}")
        
        return pd.read_parquet(dir_path, **kwargs)
    
    @log_execution_time
    def write_parquet(
        self,
        df: pd.DataFrame,
        file_path: Union[str, Path],
        compression: str = 'snappy',
        partition_cols: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """
        Write DataFrame to Parquet file.
        
        Args:
            df: DataFrame to write
            file_path: Output file path
            compression: Compression algorithm
            partition_cols: Columns for partitioning
            **kwargs: Additional arguments
        """
        path = self._resolve_path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing {len(df)} rows to Parquet: {path}")
        
        df.to_parquet(
            path,
            compression=compression,
            partition_cols=partition_cols,
            **kwargs
        )


class ExcelIngestion(FileIngestion):
    """Ingestion handler for Excel files."""
    
    @log_execution_time
    def read_excel(
        self,
        file_path: Union[str, Path],
        sheet_name: Union[str, int, List] = 0,
        **kwargs
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read Excel file.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index
            **kwargs: Additional arguments
            
        Returns:
            DataFrame or dict of DataFrames
        """
        path = self._resolve_path(file_path)
        self._validate_file(path)
        
        logger.info(f"Reading Excel file: {path}")
        
        return pd.read_excel(path, sheet_name=sheet_name, **kwargs)
    
    @log_execution_time
    def read_all_sheets(
        self,
        file_path: Union[str, Path],
        **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Read all sheets from Excel file.
        
        Args:
            file_path: Path to Excel file
            **kwargs: Additional arguments
            
        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        return self.read_excel(file_path, sheet_name=None, **kwargs)


class ProductsFileIngestion(CSVIngestion):
    """Specialized ingestion for product CSV files."""
    
    @log_execution_time
    def ingest_products(
        self,
        file_path: str = 'sample/products.csv'
    ) -> pd.DataFrame:
        """
        Ingest products from CSV file.
        
        Args:
            file_path: Path to products CSV
            
        Returns:
            Products DataFrame
        """
        df = self.read_csv(
            file_path,
            dtype={
                'product_id': str,
                'sku': str,
                'price': float,
                'stock_quantity': int
            }
        )
        
        # Data validation and cleaning
        df['product_id'] = df['product_id'].str.strip()
        df['name'] = df['name'].str.strip()
        df['price'] = df['price'].clip(lower=0)
        df['stock_quantity'] = df['stock_quantity'].clip(lower=0)
        
        logger.info(f"Ingested {len(df)} products")
        return df


class CustomersFileIngestion(CSVIngestion):
    """Specialized ingestion for customer CSV files."""
    
    @log_execution_time
    def ingest_customers(
        self,
        file_path: str = 'sample/customers.csv'
    ) -> pd.DataFrame:
        """
        Ingest customers from CSV file.
        
        Args:
            file_path: Path to customers CSV
            
        Returns:
            Customers DataFrame
        """
        df = self.read_csv(
            file_path,
            date_columns=['created_at', 'last_login'],
            dtype={
                'customer_id': str,
                'email': str
            }
        )
        
        # Data cleaning
        df['email'] = df['email'].str.lower().str.strip()
        df['first_name'] = df['first_name'].str.strip().str.title()
        df['last_name'] = df['last_name'].str.strip().str.title()
        
        logger.info(f"Ingested {len(df)} customers")
        return df


# Convenience instances
csv_ingestion = CSVIngestion()
json_ingestion = JSONIngestion()
parquet_ingestion = ParquetIngestion()
excel_ingestion = ExcelIngestion()
products_file_ingestion = ProductsFileIngestion()
customers_file_ingestion = CustomersFileIngestion()


__all__ = [
    'FileIngestion', 'CSVIngestion', 'JSONIngestion', 
    'ParquetIngestion', 'ExcelIngestion',
    'ProductsFileIngestion', 'CustomersFileIngestion',
    'csv_ingestion', 'json_ingestion', 'parquet_ingestion',
    'excel_ingestion', 'products_file_ingestion', 'customers_file_ingestion'
]
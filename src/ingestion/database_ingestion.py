"""
Database-based data ingestion module.
Handles extracting data from source databases.
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Generator
from datetime import datetime, timedelta

from src.config import settings
from src.utils.logging_utils import logger, log_execution_time
from src.utils.database_utils import (
    DatabaseConnection, QueryExecutor, source_db, source_executor
)


class DatabaseIngestion:
    """Base class for database ingestion operations."""
    
    def __init__(self, db_connection: DatabaseConnection = None):
        self.db = db_connection or source_db
        self.executor = QueryExecutor(self.db)
    
    @log_execution_time
    def extract_table(
        self,
        table_name: str,
        schema: Optional[str] = None,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract data from a database table.
        
        Args:
            table_name: Table name
            schema: Schema name
            columns: Columns to select
            where: WHERE clause condition
            order_by: ORDER BY clause
            limit: Maximum rows to return
            
        Returns:
            DataFrame with table data
        """
        cols = ', '.join(columns) if columns else '*'
        schema_prefix = f"{schema}." if schema else ""
        
        query = f"SELECT {cols} FROM {schema_prefix}{table_name}"
        
        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"
        
        logger.info(f"Extracting data from {schema_prefix}{table_name}")
        return self.executor.read_to_dataframe(query)
    
    @log_execution_time
    def extract_incremental(
        self,
        table_name: str,
        timestamp_column: str,
        last_extracted: datetime,
        schema: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Extract data incrementally based on timestamp.
        
        Args:
            table_name: Table name
            timestamp_column: Column containing timestamp
            last_extracted: Timestamp of last extraction
            schema: Schema name
            columns: Columns to select
            
        Returns:
            DataFrame with incremental data
        """
        where = f"{timestamp_column} > '{last_extracted.isoformat()}'"
        
        return self.extract_table(
            table_name=table_name,
            schema=schema,
            columns=columns,
            where=where,
            order_by=timestamp_column
        )
    
    @log_execution_time
    def extract_with_join(
        self,
        query: str,
        params: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Extract data using custom SQL query with joins.
        
        Args:
            query: SQL query with joins
            params: Query parameters
            
        Returns:
            DataFrame with query results
        """
        logger.info("Executing custom extraction query")
        return self.executor.read_to_dataframe(query, params)
    
    def extract_in_chunks(
        self,
        table_name: str,
        chunk_size: int = 10000,
        schema: Optional[str] = None,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Extract data in chunks to handle large tables.
        
        Args:
            table_name: Table name
            chunk_size: Number of rows per chunk
            schema: Schema name
            columns: Columns to select
            where: WHERE clause condition
            
        Yields:
            DataFrame chunks
        """
        cols = ', '.join(columns) if columns else '*'
        schema_prefix = f"{schema}." if schema else ""
        
        query = f"SELECT {cols} FROM {schema_prefix}{table_name}"
        if where:
            query += f" WHERE {where}"
        
        logger.info(f"Extracting {schema_prefix}{table_name} in chunks of {chunk_size}")
        
        for chunk in self.executor.read_to_dataframe(query, chunksize=chunk_size):
            yield chunk


class CustomersIngestion(DatabaseIngestion):
    """Specialized ingestion for customers data."""
    
    @log_execution_time
    def extract_customers(
        self,
        active_only: bool = False,
        include_addresses: bool = True
    ) -> pd.DataFrame:
        """
        Extract customer data with optional addresses.
        
        Args:
            active_only: Only extract active customers
            include_addresses: Include address information
            
        Returns:
            Customers DataFrame
        """
        if include_addresses:
            query = """
                SELECT 
                    c.customer_id,
                    c.email,
                    c.first_name,
                    c.last_name,
                    c.phone,
                    c.created_at,
                    c.last_login,
                    c.is_active,
                    c.customer_segment,
                    a.address_line1,
                    a.address_line2,
                    a.city,
                    a.state,
                    a.postal_code,
                    a.country
                FROM customers c
                LEFT JOIN customer_addresses a 
                    ON c.customer_id = a.customer_id 
                    AND a.is_primary = true
            """
        else:
            query = """
                SELECT 
                    customer_id,
                    email,
                    first_name,
                    last_name,
                    phone,
                    created_at,
                    last_login,
                    is_active,
                    customer_segment
                FROM customers
            """
        
        if active_only:
            query += " WHERE c.is_active = true" if include_addresses else " WHERE is_active = true"
        
        logger.info("Extracting customers data")
        return self.executor.read_to_dataframe(query)
    
    @log_execution_time
    def extract_customer_metrics(self) -> pd.DataFrame:
        """
        Extract customer metrics including order history.
        
        Returns:
            Customer metrics DataFrame
        """
        query = """
            SELECT 
                c.customer_id,
                c.email,
                c.customer_segment,
                COUNT(DISTINCT o.order_id) as total_orders,
                COALESCE(SUM(o.total_amount), 0) as total_spent,
                COALESCE(AVG(o.total_amount), 0) as avg_order_value,
                MIN(o.order_date) as first_order_date,
                MAX(o.order_date) as last_order_date,
                c.created_at as customer_since
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.email, c.customer_segment, c.created_at
        """
        
        logger.info("Extracting customer metrics")
        return self.executor.read_to_dataframe(query)


class OrdersIngestion(DatabaseIngestion):
    """Specialized ingestion for orders data."""
    
    @log_execution_time
    def extract_orders(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        status: Optional[str] = None,
        include_items: bool = False
    ) -> pd.DataFrame:
        """
        Extract orders data.
        
        Args:
            start_date: Start date filter
            end_date: End date filter
            status: Order status filter
            include_items: Include order items
            
        Returns:
            Orders DataFrame
        """
        if include_items:
            query = """
                SELECT 
                    o.order_id,
                    o.customer_id,
                    o.order_date,
                    o.status,
                    o.total_amount,
                    o.shipping_amount,
                    o.discount_amount,
                    o.payment_method,
                    o.shipping_address_id,
                    oi.order_item_id,
                    oi.product_id,
                    oi.quantity,
                    oi.unit_price,
                    oi.line_total
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
            """
        else:
            query = """
                SELECT 
                    order_id,
                    customer_id,
                    order_date,
                    status,
                    total_amount,
                    shipping_amount,
                    discount_amount,
                    payment_method,
                    shipping_address_id,
                    created_at,
                    updated_at
                FROM orders
            """
        
        conditions = []
        
        if start_date:
            conditions.append(f"o.order_date >= '{start_date.isoformat()}'" if include_items 
                            else f"order_date >= '{start_date.isoformat()}'")
        if end_date:
            conditions.append(f"o.order_date <= '{end_date.isoformat()}'" if include_items 
                            else f"order_date <= '{end_date.isoformat()}'")
        if status:
            conditions.append(f"o.status = '{status}'" if include_items 
                            else f"status = '{status}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        logger.info("Extracting orders data")
        return self.executor.read_to_dataframe(query)
    
    @log_execution_time
    def extract_recent_orders(self, days: int = 7) -> pd.DataFrame:
        """
        Extract orders from the last N days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            Recent orders DataFrame
        """
        start_date = datetime.now() - timedelta(days=days)
        return self.extract_orders(start_date=start_date)
    
    @log_execution_time
    def extract_order_summary(
        self,
        group_by: str = 'day'
    ) -> pd.DataFrame:
        """
        Extract order summary aggregations.
        
        Args:
            group_by: Grouping period ('day', 'week', 'month')
            
        Returns:
            Order summary DataFrame
        """
        date_trunc = {
            'day': "DATE_TRUNC('day', order_date)",
            'week': "DATE_TRUNC('week', order_date)",
            'month': "DATE_TRUNC('month', order_date)"
        }.get(group_by, "DATE_TRUNC('day', order_date)")
        
        query = f"""
            SELECT 
                {date_trunc} as period,
                COUNT(*) as order_count,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value,
                SUM(shipping_amount) as total_shipping,
                SUM(discount_amount) as total_discounts
            FROM orders
            GROUP BY {date_trunc}
            ORDER BY period DESC
        """
        
        logger.info(f"Extracting order summary grouped by {group_by}")
        return self.executor.read_to_dataframe(query)


class ProductsIngestion(DatabaseIngestion):
    """Specialized ingestion for products data."""
    
    @log_execution_time
    def extract_products(
        self,
        category: Optional[str] = None,
        active_only: bool = True,
        include_inventory: bool = True
    ) -> pd.DataFrame:
        """
        Extract products data.
        
        Args:
            category: Product category filter
            active_only: Only extract active products
            include_inventory: Include inventory information
            
        Returns:
            Products DataFrame
        """
        if include_inventory:
            query = """
                SELECT 
                    p.product_id,
                    p.sku,
                    p.name,
                    p.description,
                    p.category,
                    p.subcategory,
                    p.price,
                    p.cost,
                    p.is_active,
                    i.stock_quantity,
                    i.reserved_quantity,
                    i.warehouse_location
                FROM products p
                LEFT JOIN inventory i ON p.product_id = i.product_id
            """
        else:
            query = """
                SELECT 
                    product_id,
                    sku,
                    name,
                    description,
                    category,
                    subcategory,
                    price,
                    cost,
                    is_active,
                    created_at,
                    updated_at
                FROM products
            """
        
        conditions = []
        
        if category:
            conditions.append(f"p.category = '{category}'" if include_inventory 
                            else f"category = '{category}'")
        if active_only:
            conditions.append("p.is_active = true" if include_inventory 
                            else "is_active = true")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        logger.info("Extracting products data")
        return self.executor.read_to_dataframe(query)
    
    @log_execution_time
    def extract_product_performance(self) -> pd.DataFrame:
        """
        Extract product sales performance metrics.
        
        Returns:
            Product performance DataFrame
        """
        query = """
            SELECT 
                p.product_id,
                p.sku,
                p.name,
                p.category,
                p.price,
                p.cost,
                COUNT(DISTINCT oi.order_id) as order_count,
                SUM(oi.quantity) as units_sold,
                SUM(oi.line_total) as total_revenue,
                SUM(oi.quantity * p.cost) as total_cost,
                SUM(oi.line_total) - SUM(oi.quantity * p.cost) as gross_profit,
                AVG(oi.unit_price) as avg_selling_price
            FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE o.status NOT IN ('cancelled', 'refunded')
               OR o.status IS NULL
            GROUP BY p.product_id, p.sku, p.name, p.category, p.price, p.cost
        """
        
        logger.info("Extracting product performance metrics")
        return self.executor.read_to_dataframe(query)


# Convenience instances
customers_ingestion = CustomersIngestion()
orders_ingestion = OrdersIngestion()
products_ingestion = ProductsIngestion()


__all__ = [
    'DatabaseIngestion',
    'CustomersIngestion',
    'OrdersIngestion',
    'ProductsIngestion',
    'customers_ingestion',
    'orders_ingestion',
    'products_ingestion'
]
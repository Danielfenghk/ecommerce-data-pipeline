"""
Schema Definitions Module
Defines schemas for dimensions, facts, and data marts
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DataType(Enum):
    """SQL data types"""
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    REAL = "REAL"
    DOUBLE = "DOUBLE PRECISION"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    CHAR = "CHAR"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"
    JSON = "JSON"
    JSONB = "JSONB"
    UUID = "UUID"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"


@dataclass
class Column:
    """Column definition"""
    name: str
    data_type: DataType
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    default: Optional[str] = None
    primary_key: bool = False
    unique: bool = False
    foreign_key: Optional[str] = None
    check_constraint: Optional[str] = None
    comment: Optional[str] = None
    
    def to_sql(self) -> str:
        """Generate SQL column definition"""
        type_str = self.data_type.value
        
        if self.length:
            type_str = f"{type_str}({self.length})"
        elif self.precision and self.scale:
            type_str = f"{type_str}({self.precision}, {self.scale})"
        elif self.precision:
            type_str = f"{type_str}({self.precision})"
        
        parts = [self.name, type_str]
        
        if self.primary_key:
            parts.append("PRIMARY KEY")
        if not self.nullable and not self.primary_key:
            parts.append("NOT NULL")
        if self.unique and not self.primary_key:
            parts.append("UNIQUE")
        if self.default:
            parts.append(f"DEFAULT {self.default}")
        if self.check_constraint:
            parts.append(f"CHECK ({self.check_constraint})")
        
        return " ".join(parts)


@dataclass
class Index:
    """Index definition"""
    name: str
    columns: List[str]
    unique: bool = False
    method: str = "btree"
    include: Optional[List[str]] = None
    where: Optional[str] = None
    
    def to_sql(self, table_name: str, schema: str = "public") -> str:
        """Generate SQL index definition"""
        unique_str = "UNIQUE" if self.unique else ""
        columns_str = ", ".join(self.columns)
        
        sql = f"CREATE {unique_str} INDEX IF NOT EXISTS {self.name} ON {schema}.{table_name} USING {self.method} ({columns_str})"
        
        if self.include:
            include_str = ", ".join(self.include)
            sql += f" INCLUDE ({include_str})"
        
        if self.where:
            sql += f" WHERE {self.where}"
        
        return sql


@dataclass
class ForeignKey:
    """Foreign key definition"""
    name: str
    columns: List[str]
    reference_table: str
    reference_columns: List[str]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"
    
    def to_sql(self) -> str:
        """Generate SQL foreign key constraint"""
        cols = ", ".join(self.columns)
        ref_cols = ", ".join(self.reference_columns)
        
        return f"""
            CONSTRAINT {self.name}
            FOREIGN KEY ({cols})
            REFERENCES {self.reference_table} ({ref_cols})
            ON DELETE {self.on_delete}
            ON UPDATE {self.on_update}
        """


class DimensionSchema:
    """Schema definitions for dimension tables"""
    
    @staticmethod
    def dim_customer() -> Dict[str, Any]:
        """Customer dimension schema"""
        return {
            'table_name': 'dim_customer',
            'schema': 'dim',
            'columns': [
                Column('customer_sk', DataType.BIGSERIAL, primary_key=True, comment='Surrogate key'),
                Column('customer_id', DataType.VARCHAR, length=50, nullable=False, unique=True, comment='Business key'),
                Column('first_name', DataType.VARCHAR, length=100, comment='Customer first name'),
                Column('last_name', DataType.VARCHAR, length=100, comment='Customer last name'),
                Column('full_name', DataType.VARCHAR, length=200, comment='Customer full name'),
                Column('email', DataType.VARCHAR, length=255, comment='Email address'),
                Column('phone', DataType.VARCHAR, length=50, comment='Phone number'),
                Column('address_line1', DataType.VARCHAR, length=255, comment='Street address'),
                Column('address_line2', DataType.VARCHAR, length=255, comment='Address line 2'),
                Column('city', DataType.VARCHAR, length=100, comment='City'),
                Column('state', DataType.VARCHAR, length=100, comment='State/Province'),
                Column('country', DataType.VARCHAR, length=100, comment='Country'),
                Column('postal_code', DataType.VARCHAR, length=20, comment='Postal/ZIP code'),
                Column('birth_date', DataType.DATE, comment='Date of birth'),
                Column('age', DataType.INTEGER, comment='Customer age'),
                Column('age_group', DataType.VARCHAR, length=50, comment='Age group category'),
                Column('gender', DataType.VARCHAR, length=20, comment='Gender'),
                Column('registration_date', DataType.DATE, comment='Account registration date'),
                Column('customer_segment', DataType.VARCHAR, length=50, comment='Customer segment'),
                Column('is_active', DataType.BOOLEAN, default='true', comment='Active status'),
                Column('effective_from', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='SCD2 effective from'),
                Column('effective_to', DataType.TIMESTAMP, default="'9999-12-31'", comment='SCD2 effective to'),
                Column('is_current', DataType.BOOLEAN, default='true', comment='Current record flag'),
                Column('etl_loaded_at', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='ETL load timestamp'),
                Column('etl_updated_at', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='ETL update timestamp'),
            ],
            'indexes': [
                Index('idx_dim_customer_id', ['customer_id']),
                Index('idx_dim_customer_email', ['email']),
                Index('idx_dim_customer_segment', ['customer_segment']),
                Index('idx_dim_customer_current', ['is_current'], where='is_current = true'),
            ],
            'comment': 'Customer dimension table with SCD Type 2 support'
        }
    
    @staticmethod
    def dim_product() -> Dict[str, Any]:
        """Product dimension schema"""
        return {
            'table_name': 'dim_product',
            'schema': 'dim',
            'columns': [
                Column('product_sk', DataType.BIGSERIAL, primary_key=True, comment='Surrogate key'),
                Column('product_id', DataType.VARCHAR, length=50, nullable=False, unique=True, comment='Business key'),
                Column('product_name', DataType.VARCHAR, length=500, comment='Product name'),
                Column('product_description', DataType.TEXT, comment='Product description'),
                Column('sku', DataType.VARCHAR, length=100, comment='Stock Keeping Unit'),
                Column('brand', DataType.VARCHAR, length=200, comment='Brand name'),
                Column('category', DataType.VARCHAR, length=200, comment='Product category'),
                Column('subcategory', DataType.VARCHAR, length=200, comment='Product subcategory'),
                Column('unit_price', DataType.DECIMAL, precision=12, scale=2, comment='Unit price'),
                Column('unit_cost', DataType.DECIMAL, precision=12, scale=2, comment='Unit cost'),
                Column('profit_margin', DataType.DECIMAL, precision=8, scale=4, comment='Profit margin percentage'),
                Column('weight', DataType.DECIMAL, precision=10, scale=2, comment='Product weight'),
                Column('weight_unit', DataType.VARCHAR, length=20, comment='Weight unit'),
                Column('dimensions', DataType.VARCHAR, length=100, comment='Product dimensions'),
                Column('color', DataType.VARCHAR, length=50, comment='Product color'),
                Column('size', DataType.VARCHAR, length=50, comment='Product size'),
                Column('is_active', DataType.BOOLEAN, default='true', comment='Active status'),
                Column('launch_date', DataType.DATE, comment='Product launch date'),
                Column('discontinue_date', DataType.DATE, comment='Discontinuation date'),
                Column('supplier_id', DataType.VARCHAR, length=50, comment='Supplier ID'),
                Column('effective_from', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='SCD2 effective from'),
                Column('effective_to', DataType.TIMESTAMP, default="'9999-12-31'", comment='SCD2 effective to'),
                Column('is_current', DataType.BOOLEAN, default='true', comment='Current record flag'),
                Column('etl_loaded_at', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='ETL load timestamp'),
                Column('etl_updated_at', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='ETL update timestamp'),
            ],
            'indexes': [
                Index('idx_dim_product_id', ['product_id']),
                Index('idx_dim_product_sku', ['sku']),
                Index('idx_dim_product_category', ['category', 'subcategory']),
                Index('idx_dim_product_brand', ['brand']),
                Index('idx_dim_product_current', ['is_current'], where='is_current = true'),
            ],
            'comment': 'Product dimension table with SCD Type 2 support'
        }
    
    @staticmethod
    def dim_date() -> Dict[str, Any]:
        """Date dimension schema"""
        return {
            'table_name': 'dim_date',
            'schema': 'dim',
            'columns': [
                Column('date_key', DataType.INTEGER, primary_key=True, comment='Date key (YYYYMMDD format)'),
                Column('full_date', DataType.DATE, nullable=False, unique=True, comment='Full date'),
                Column('year', DataType.SMALLINT, nullable=False, comment='Year'),
                Column('quarter', DataType.SMALLINT, nullable=False, comment='Quarter (1-4)'),
                Column('month', DataType.SMALLINT, nullable=False, comment='Month (1-12)'),
                Column('month_name', DataType.VARCHAR, length=20, comment='Month name'),
                Column('month_abbr', DataType.VARCHAR, length=3, comment='Month abbreviation'),
                Column('week_of_year', DataType.SMALLINT, comment='Week of year (1-53)'),
                Column('day_of_year', DataType.SMALLINT, comment='Day of year (1-366)'),
                Column('day_of_month', DataType.SMALLINT, comment='Day of month (1-31)'),
                Column('day_of_week', DataType.SMALLINT, comment='Day of week (0-6, 0=Monday)'),
                Column('day_name', DataType.VARCHAR, length=20, comment='Day name'),
                Column('day_abbr', DataType.VARCHAR, length=3, comment='Day abbreviation'),
                Column('is_weekend', DataType.BOOLEAN, comment='Weekend flag'),
                Column('is_holiday', DataType.BOOLEAN, default='false', comment='Holiday flag'),
                Column('holiday_name', DataType.VARCHAR, length=100, comment='Holiday name'),
                Column('is_month_start', DataType.BOOLEAN, comment='First day of month'),
                Column('is_month_end', DataType.BOOLEAN, comment='Last day of month'),
                Column('is_quarter_start', DataType.BOOLEAN, comment='First day of quarter'),
                Column('is_quarter_end', DataType.BOOLEAN, comment='Last day of quarter'),
                Column('is_year_start', DataType.BOOLEAN, comment='First day of year'),
                Column('is_year_end', DataType.BOOLEAN, comment='Last day of year'),
                Column('fiscal_year', DataType.SMALLINT, comment='Fiscal year'),
                Column('fiscal_quarter', DataType.SMALLINT, comment='Fiscal quarter'),
                Column('fiscal_month', DataType.SMALLINT, comment='Fiscal month'),
                Column('season', DataType.VARCHAR, length=20, comment='Season'),
            ],
            'indexes': [
                Index('idx_dim_date_full_date', ['full_date']),
                Index('idx_dim_date_year_month', ['year', 'month']),
                Index('idx_dim_date_year_quarter', ['year', 'quarter']),
            ],
            'comment': 'Date dimension table for time-based analysis'
        }
    
    @staticmethod
    def dim_location() -> Dict[str, Any]:
        """Location dimension schema"""
        return {
            'table_name': 'dim_location',
            'schema': 'dim',
            'columns': [
                Column('location_sk', DataType.BIGSERIAL, primary_key=True, comment='Surrogate key'),
                Column('location_id', DataType.VARCHAR, length=50, unique=True, comment='Business key'),
                Column('country', DataType.VARCHAR, length=100, comment='Country name'),
                Column('country_code', DataType.VARCHAR, length=3, comment='ISO country code'),
                Column('region', DataType.VARCHAR, length=100, comment='Region/Province'),
                Column('state', DataType.VARCHAR, length=100, comment='State'),
                Column('city', DataType.VARCHAR, length=200, comment='City'),
                Column('postal_code', DataType.VARCHAR, length=20, comment='Postal code'),
                Column('latitude', DataType.DECIMAL, precision=10, scale=7, comment='Latitude'),
                Column('longitude', DataType.DECIMAL, precision=10, scale=7, comment='Longitude'),
                Column('timezone', DataType.VARCHAR, length=50, comment='Timezone'),
                Column('is_active', DataType.BOOLEAN, default='true', comment='Active status'),
                Column('etl_loaded_at', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='ETL load timestamp'),
            ],
            'indexes': [
                Index('idx_dim_location_country', ['country']),
                Index('idx_dim_location_state', ['state']),
                Index('idx_dim_location_city', ['city']),
                Index('idx_dim_location_postal', ['postal_code']),
            ],
            'comment': 'Location dimension table for geographical analysis'
        }


class FactSchema:
    """Schema definitions for fact tables"""
    
    @staticmethod
    def fact_orders() -> Dict[str, Any]:
        """Orders fact table schema"""
        return {
            'table_name': 'fact_orders',
            'schema': 'fact',
            'columns': [
                Column('order_sk', DataType.BIGSERIAL, primary_key=True, comment='Surrogate key'),
                Column('order_id', DataType.VARCHAR, length=50, nullable=False, comment='Order ID'),
                Column('order_line_number', DataType.INTEGER, comment='Order line number'),
                Column('date_key', DataType.INTEGER, nullable=False, comment='FK to dim_date'),
                Column('customer_sk', DataType.BIGINT, nullable=False, comment='FK to dim_customer'),
                Column('product_sk', DataType.BIGINT, nullable=False, comment='FK to dim_product'),
                Column('location_sk', DataType.BIGINT, comment='FK to dim_location'),
                Column('quantity', DataType.INTEGER, nullable=False, comment='Order quantity'),
                Column('unit_price', DataType.DECIMAL, precision=12, scale=2, comment='Unit price'),
                Column('unit_cost', DataType.DECIMAL, precision=12, scale=2, comment='Unit cost'),
                Column('discount_amount', DataType.DECIMAL, precision=12, scale=2, default='0', comment='Discount amount'),
                Column('discount_percent', DataType.DECIMAL, precision=5, scale=2, default='0', comment='Discount percentage'),
                Column('tax_amount', DataType.DECIMAL, precision=12, scale=2, default='0', comment='Tax amount'),
                Column('shipping_amount', DataType.DECIMAL, precision=12, scale=2, default='0', comment='Shipping amount'),
                Column('gross_amount', DataType.DECIMAL, precision=12, scale=2, comment='Gross amount'),
                Column('net_amount', DataType.DECIMAL, precision=12, scale=2, comment='Net amount'),
                Column('profit_amount', DataType.DECIMAL, precision=12, scale=2, comment='Profit amount'),
                Column('order_status', DataType.VARCHAR, length=50, comment='Order status'),
                Column('payment_method', DataType.VARCHAR, length=50, comment='Payment method'),
                Column('order_timestamp', DataType.TIMESTAMP, comment='Order timestamp'),
                Column('ship_timestamp', DataType.TIMESTAMP, comment='Ship timestamp'),
                Column('delivery_timestamp', DataType.TIMESTAMP, comment='Delivery timestamp'),
                Column('days_to_ship', DataType.INTEGER, comment='Days between order and ship'),
                Column('days_to_deliver', DataType.INTEGER, comment='Days between ship and deliver'),
                Column('etl_loaded_at', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='ETL load timestamp'),
            ],
            'indexes': [
                Index('idx_fact_orders_date', ['date_key']),
                Index('idx_fact_orders_customer', ['customer_sk']),
                Index('idx_fact_orders_product', ['product_sk']),
                Index('idx_fact_orders_status', ['order_status']),
                Index('idx_fact_orders_timestamp', ['order_timestamp']),
                Index('idx_fact_orders_order_id', ['order_id']),
            ],
            'foreign_keys': [
                ForeignKey('fk_fact_orders_date', ['date_key'], 'dim.dim_date', ['date_key']),
                ForeignKey('fk_fact_orders_customer', ['customer_sk'], 'dim.dim_customer', ['customer_sk']),
                ForeignKey('fk_fact_orders_product', ['product_sk'], 'dim.dim_product', ['product_sk']),
            ],
            'partition_by': 'RANGE (date_key)',
            'comment': 'Orders fact table - grain is order line item'
        }
    
    @staticmethod
    def fact_daily_sales() -> Dict[str, Any]:
        """Daily sales aggregate fact table"""
        return {
            'table_name': 'fact_daily_sales',
            'schema': 'fact',
            'columns': [
                Column('daily_sales_sk', DataType.BIGSERIAL, primary_key=True, comment='Surrogate key'),
                Column('date_key', DataType.INTEGER, nullable=False, comment='FK to dim_date'),
                Column('product_sk', DataType.BIGINT, comment='FK to dim_product'),
                Column('location_sk', DataType.BIGINT, comment='FK to dim_location'),
                Column('total_orders', DataType.INTEGER, comment='Total number of orders'),
                Column('total_quantity', DataType.INTEGER, comment='Total quantity sold'),
                Column('total_customers', DataType.INTEGER, comment='Unique customers'),
                Column('gross_revenue', DataType.DECIMAL, precision=15, scale=2, comment='Gross revenue'),
                Column('net_revenue', DataType.DECIMAL, precision=15, scale=2, comment='Net revenue'),
                Column('total_cost', DataType.DECIMAL, precision=15, scale=2, comment='Total cost'),
                Column('total_profit', DataType.DECIMAL, precision=15, scale=2, comment='Total profit'),
                Column('total_discounts', DataType.DECIMAL, precision=15, scale=2, comment='Total discounts'),
                Column('avg_order_value', DataType.DECIMAL, precision=12, scale=2, comment='Average order value'),
                Column('avg_items_per_order', DataType.DECIMAL, precision=8, scale=2, comment='Avg items per order'),
                Column('etl_loaded_at', DataType.TIMESTAMP, default='CURRENT_TIMESTAMP', comment='ETL load timestamp'),
            ],
            'indexes': [
                Index('idx_fact_daily_sales_date', ['date_key']),
                Index('idx_fact_daily_sales_product', ['product_sk']),
                Index('idx_fact_daily_sales_composite', ['date_key', 'product_sk', 'location_sk'], unique=True),
            ],
            'comment': 'Daily sales aggregate fact table'
        }


class DataMartSchema:
    """Schema definitions for data marts"""
    
    @staticmethod
    def customer_360() -> Dict[str, Any]:
        """Customer 360 view schema"""
        return {
            'view_name': 'mv_customer_360',
            'schema': 'mart',
            'is_materialized': True,
            'sql': """
                SELECT 
                    c.customer_sk,
                    c.customer_id,
                    c.full_name,
                    c.email,
                    c.customer_segment,
                    COUNT(DISTINCT o.order_id) as total_orders,
                    SUM(o.quantity) as total_items_purchased,
                    SUM(o.net_amount) as total_revenue,
                    AVG(o.net_amount) as avg_order_value,
                    MIN(d.full_date) as first_order_date,
                    MAX(d.full_date) as last_order_date,
                    MAX(d.full_date) - MIN(d.full_date) as customer_lifetime_days,
                    CURRENT_DATE - MAX(d.full_date) as days_since_last_order
                FROM dim.dim_customer c
                LEFT JOIN fact.fact_orders o ON c.customer_sk = o.customer_sk
                LEFT JOIN dim.dim_date d ON o.date_key = d.date_key
                WHERE c.is_current = true
                GROUP BY c.customer_sk, c.customer_id, c.full_name, c.email, c.customer_segment
            """,
            'indexes': [
                Index('idx_mv_customer_360_id', ['customer_id']),
                Index('idx_mv_customer_360_segment', ['customer_segment']),
            ],
            'comment': 'Materialized view for customer 360 analysis'
        }
    
    @staticmethod
    def product_performance() -> Dict[str, Any]:
        """Product performance view schema"""
        return {
            'view_name': 'mv_product_performance',
            'schema': 'mart',
            'is_materialized': True,
            'sql': """
                SELECT 
                    p.product_sk,
                    p.product_id,
                    p.product_name,
                    p.category,
                    p.brand,
                    COUNT(DISTINCT o.order_id) as total_orders,
                    SUM(o.quantity) as total_quantity_sold,
                    SUM(o.net_amount) as total_revenue,
                    SUM(o.profit_amount) as total_profit,
                    AVG(o.profit_amount / NULLIF(o.net_amount, 0) * 100) as avg_margin_percent,
                    COUNT(DISTINCT o.customer_sk) as unique_customers,
                    MIN(d.full_date) as first_sale_date,
                    MAX(d.full_date) as last_sale_date
                FROM dim.dim_product p
                LEFT JOIN fact.fact_orders o ON p.product_sk = o.product_sk
                LEFT JOIN dim.dim_date d ON o.date_key = d.date_key
                WHERE p.is_current = true
                GROUP BY p.product_sk, p.product_id, p.product_name, p.category, p.brand
            """,
            'indexes': [
                Index('idx_mv_product_perf_id', ['product_id']),
                Index('idx_mv_product_perf_category', ['category']),
            ],
            'comment': 'Materialized view for product performance analysis'
        }


class SchemaManager:
    """Manages schema creation and updates"""
    
    def __init__(self, warehouse):
        self.warehouse = warehouse
    
    def create_table(self, schema_def: Dict[str, Any]) -> None:
        """Create a table from schema definition"""
        table_name = schema_def['table_name']
        schema = schema_def['schema']
        columns = schema_def['columns']
        
        # Create schema if not exists
        self.warehouse.create_schema(schema)
        
        # Build column definitions
        column_defs = [col.to_sql() for col in columns]
        columns_sql = ",\n    ".join(column_defs)
        
        # Create table SQL
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {columns_sql}
            )
        """
        
        # Add partitioning if specified
        if 'partition_by' in schema_def:
            create_sql += f" PARTITION BY {schema_def['partition_by']}"
        
        self.warehouse.execute_query(create_sql)
        logger.info(f"Table {schema}.{table_name} created")
        
        # Create indexes
        if 'indexes' in schema_def:
            for index in schema_def['indexes']:
                index_sql = index.to_sql(table_name, schema)
                try:
                    self.warehouse.execute_query(index_sql)
                    logger.info(f"Index {index.name} created")
                except Exception as e:
                    logger.warning(f"Failed to create index {index.name}: {e}")
        
        # Add table comment
        if 'comment' in schema_def:
            comment_sql = f"COMMENT ON TABLE {schema}.{table_name} IS '{schema_def['comment']}'"
            self.warehouse.execute_query(comment_sql)
    
    def create_materialized_view(self, view_def: Dict[str, Any]) -> None:
        """Create a materialized view from definition"""
        view_name = view_def['view_name']
        schema = view_def['schema']
        sql = view_def['sql']
        
        # Create schema if not exists
        self.warehouse.create_schema(schema)
        
        # Create materialized view
        create_sql = f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.{view_name}
            AS {sql}
        """
        
        self.warehouse.execute_query(create_sql)
        logger.info(f"Materialized view {schema}.{view_name} created")
        
        # Create indexes
        if 'indexes' in view_def:
            for index in view_def['indexes']:
                index_sql = index.to_sql(view_name, schema)
                try:
                    self.warehouse.execute_query(index_sql)
                except Exception as e:
                    logger.warning(f"Failed to create index: {e}")
    
    def create_all_schemas(self) -> None:
        """Create all dimension, fact, and mart schemas"""
        # Create dimensions
        for dim_schema in [
            DimensionSchema.dim_customer(),
            DimensionSchema.dim_product(),
            DimensionSchema.dim_date(),
            DimensionSchema.dim_location(),
        ]:
            self.create_table(dim_schema)
        
        # Create facts
        for fact_schema in [
            FactSchema.fact_orders(),
            FactSchema.fact_daily_sales(),
        ]:
            self.create_table(fact_schema)
        
        # Create marts
        for mart_schema in [
            DataMartSchema.customer_360(),
            DataMartSchema.product_performance(),
        ]:
            self.create_materialized_view(mart_schema)
        
        logger.info("All schemas created successfully")


if __name__ == "__main__":
    # Example usage
    print("Dimension Schemas:")
    print(f"  - dim_customer: {len(DimensionSchema.dim_customer()['columns'])} columns")
    print(f"  - dim_product: {len(DimensionSchema.dim_product()['columns'])} columns")
    print(f"  - dim_date: {len(DimensionSchema.dim_date()['columns'])} columns")
    
    print("\nFact Schemas:")
    print(f"  - fact_orders: {len(FactSchema.fact_orders()['columns'])} columns")
    print(f"  - fact_daily_sales: {len(FactSchema.fact_daily_sales()['columns'])} columns")
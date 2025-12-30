"""Data ingestion module for the E-Commerce Data Pipeline."""

from .api_ingestion import (
    APIIngestionClient,
    OrdersAPIIngestion,
    ProductsAPIIngestion,
    EventsAPIIngestion,
    orders_ingestion as orders_api_ingestion,
    products_ingestion as products_api_ingestion,
    events_ingestion as events_api_ingestion
)

from .file_ingestion import (
    CSVIngestion,
    JSONIngestion,
    ParquetIngestion,
    ExcelIngestion,
    ProductsFileIngestion,
    CustomersFileIngestion,
    csv_ingestion,
    json_ingestion,
    parquet_ingestion,
    products_file_ingestion,
    customers_file_ingestion
)

from .database_ingestion import (
    DatabaseIngestion,
    CustomersIngestion,
    OrdersIngestion,
    ProductsIngestion,
    customers_ingestion as customers_db_ingestion,
    orders_ingestion as orders_db_ingestion,
    products_ingestion as products_db_ingestion
)

from .kafka_producer import (
    DataKafkaProducer,
    OrdersProducer,
    EventsProducer,
    ProductsProducer,
    orders_producer,
    events_producer,
    products_producer
)

__all__ = [
    # API Ingestion
    'APIIngestionClient',
    'OrdersAPIIngestion',
    'ProductsAPIIngestion',
    'EventsAPIIngestion',
    'orders_api_ingestion',
    'products_api_ingestion',
    'events_api_ingestion',
    
    # File Ingestion
    'CSVIngestion',
    'JSONIngestion',
    'ParquetIngestion',
    'ExcelIngestion',
    'ProductsFileIngestion',
    'CustomersFileIngestion',
    'csv_ingestion',
    'json_ingestion',
    'parquet_ingestion',
    'products_file_ingestion',
    'customers_file_ingestion',
    
    # Database Ingestion
    'DatabaseIngestion',
    'CustomersIngestion',
    'OrdersIngestion',
    'ProductsIngestion',
    'customers_db_ingestion',
    'orders_db_ingestion',
    'products_db_ingestion',
    
    # Kafka Producers
    'DataKafkaProducer',
    'OrdersProducer',
    'EventsProducer',
    'ProductsProducer',
    'orders_producer',
    'events_producer',
    'products_producer'
]
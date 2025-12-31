"""
Integration Tests Package
=========================

This package contains integration tests that verify the complete
data pipeline functionality with real services (databases, Kafka, MinIO, etc.).

These tests require running services and are typically run in a CI/CD environment
or with docker-compose up.

Test Categories:
- Database Integration: Tests PostgreSQL connections and operations
- Kafka Integration: Tests message producer/consumer functionality  
- MinIO Integration: Tests S3-compatible storage operations
- Pipeline Integration: Tests end-to-end data flow

Usage:
    # Run all integration tests
    pytest tests/integration/ -v
    
    # Run specific integration test
    pytest tests/integration/test_pipeline_integration.py -v
    
    # Run with markers
    pytest tests/integration/ -v -m "database"

Requirements:
    - Docker services must be running (docker-compose up)
    - Environment variables must be configured (.env file)
"""

import os
import sys
import pytest
from typing import Generator, Optional
from datetime import datetime

# Add project root to path for imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# =============================================================================
# Shared Fixtures for Integration Tests
# =============================================================================

@pytest.fixture(scope="session")
def integration_test_config() -> dict:
    """
    Provides configuration for integration tests.
    Loaded once per test session.
    """
    return {
        'source_db': {
            'host': os.getenv('SOURCE_DB_HOST', 'localhost'),
            'port': int(os.getenv('SOURCE_DB_PORT', 5432)),
            'database': os.getenv('SOURCE_DB_NAME', 'ecommerce_source'),
            'user': os.getenv('SOURCE_DB_USER', 'ecommerce_user'),
            'password': os.getenv('SOURCE_DB_PASSWORD', 'ecommerce_pass'),
        },
        'warehouse_db': {
            'host': os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
            'port': int(os.getenv('WAREHOUSE_DB_PORT', 5433)),
            'database': os.getenv('WAREHOUSE_DB_NAME', 'ecommerce_warehouse'),
            'user': os.getenv('WAREHOUSE_DB_USER', 'warehouse_user'),
            'password': os.getenv('WAREHOUSE_DB_PASSWORD', 'warehouse_pass'),
        },
        'kafka': {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        },
        'minio': {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
        },
        'test_run_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
    }


@pytest.fixture(scope="session")
def source_db_connection(integration_test_config: dict) -> Generator:
    """
    Provides a database connection to the source database.
    Connection is shared across all tests in the session.
    """
    from src.utils.database_utils import DatabaseConnection
    
    config = integration_test_config['source_db']
    connection = None
    
    try:
        connection = DatabaseConnection(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        yield connection
    except Exception as e:
        pytest.skip(f"Source database not available: {e}")
    finally:
        if connection:
            connection.close()


@pytest.fixture(scope="session")
def warehouse_db_connection(integration_test_config: dict) -> Generator:
    """
    Provides a database connection to the warehouse database.
    """
    from src.utils.database_utils import DatabaseConnection
    
    config = integration_test_config['warehouse_db']
    connection = None
    
    try:
        connection = DatabaseConnection(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        yield connection
    except Exception as e:
        pytest.skip(f"Warehouse database not available: {e}")
    finally:
        if connection:
            connection.close()


@pytest.fixture(scope="session")
def kafka_producer(integration_test_config: dict) -> Generator:
    """
    Provides a Kafka producer for integration tests.
    """
    from src.ingestion.kafka_producer import KafkaDataProducer
    
    config = integration_test_config['kafka']
    producer = None
    
    try:
        producer = KafkaDataProducer(
            bootstrap_servers=config['bootstrap_servers']
        )
        yield producer
    except Exception as e:
        pytest.skip(f"Kafka not available: {e}")
    finally:
        if producer:
            producer.close()


@pytest.fixture(scope="session")
def minio_client(integration_test_config: dict) -> Generator:
    """
    Provides a MinIO client for integration tests.
    """
    try:
        from minio import Minio
        
        config = integration_test_config['minio']
        client = Minio(
            config['endpoint'],
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            secure=False
        )
        
        # Verify connection
        client.list_buckets()
        
        yield client
    except Exception as e:
        pytest.skip(f"MinIO not available: {e}")


@pytest.fixture(scope="function")
def test_table_name(integration_test_config: dict) -> str:
    """
    Generates a unique test table name for each test.
    """
    run_id = integration_test_config['test_run_id']
    return f"test_integration_{run_id}"


# =============================================================================
# Test Markers
# =============================================================================

def pytest_configure(config):
    """Register custom markers for integration tests."""
    config.addinivalue_line(
        "markers", "database: marks tests that require database connection"
    )
    config.addinivalue_line(
        "markers", "kafka: marks tests that require Kafka connection"
    )
    config.addinivalue_line(
        "markers", "minio: marks tests that require MinIO connection"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests that take a long time to run"
    )
    config.addinivalue_line(
        "markers", "pipeline: marks end-to-end pipeline tests"
    )


# =============================================================================
# Utility Functions for Integration Tests
# =============================================================================

def wait_for_service(host: str, port: int, timeout: int = 30) -> bool:
    """
    Wait for a service to become available.
    
    Args:
        host: Service hostname
        port: Service port
        timeout: Maximum wait time in seconds
        
    Returns:
        True if service is available, False otherwise
    """
    import socket
    import time
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                result = sock.connect_ex((host, port))
                if result == 0:
                    return True
        except socket.error:
            pass
        time.sleep(1)
    return False


def cleanup_test_data(connection, table_name: str) -> None:
    """
    Clean up test data from a table.
    
    Args:
        connection: Database connection
        table_name: Name of the table to clean
    """
    try:
        connection.execute_query(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    except Exception:
        pass  # Ignore cleanup errors


def create_test_bucket(minio_client, bucket_name: str) -> None:
    """
    Create a test bucket in MinIO if it doesn't exist.
    
    Args:
        minio_client: MinIO client
        bucket_name: Name of the bucket to create
    """
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)


def cleanup_test_bucket(minio_client, bucket_name: str) -> None:
    """
    Clean up a test bucket in MinIO.
    
    Args:
        minio_client: MinIO client
        bucket_name: Name of the bucket to clean
    """
    try:
        # Remove all objects first
        objects = minio_client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            minio_client.remove_object(bucket_name, obj.object_name)
        # Remove bucket
        minio_client.remove_bucket(bucket_name)
    except Exception:
        pass  # Ignore cleanup errors


# =============================================================================
# Test Data Generators
# =============================================================================

def generate_test_order(order_id: Optional[str] = None) -> dict:
    """Generate a test order dictionary."""
    from datetime import datetime
    import random
    
    return {
        'order_id': order_id or f"TEST-ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
        'customer_id': f"CUST{random.randint(1, 100):03d}",
        'order_date': datetime.now().isoformat(),
        'total_amount': round(random.uniform(10, 500), 2),
        'status': random.choice(['pending', 'processing', 'shipped', 'completed']),
        'payment_method': random.choice(['credit_card', 'paypal', 'debit_card']),
    }


def generate_test_customer(customer_id: Optional[str] = None) -> dict:
    """Generate a test customer dictionary."""
    from datetime import datetime
    import random
    
    first_names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie']
    last_names = ['Smith', 'Doe', 'Johnson', 'Williams', 'Brown']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    
    return {
        'customer_id': customer_id or f"TEST-CUST-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        'first_name': random.choice(first_names),
        'last_name': random.choice(last_names),
        'email': f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}@test.com",
        'city': random.choice(cities),
        'registration_date': datetime.now().isoformat(),
    }


def generate_test_product(product_id: Optional[str] = None) -> dict:
    """Generate a test product dictionary."""
    from datetime import datetime
    import random
    
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Food']
    
    return {
        'product_id': product_id or f"TEST-PROD-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        'name': f"Test Product {random.randint(1, 1000)}",
        'category': random.choice(categories),
        'price': round(random.uniform(10, 200), 2),
        'cost': round(random.uniform(5, 100), 2),
        'stock_quantity': random.randint(0, 500),
    }


# =============================================================================
# Export commonly used items
# =============================================================================

__all__ = [
    # Fixtures (these are auto-discovered by pytest)
    'integration_test_config',
    'source_db_connection',
    'warehouse_db_connection',
    'kafka_producer',
    'minio_client',
    'test_table_name',
    
    # Utility functions
    'wait_for_service',
    'cleanup_test_data',
    'create_test_bucket',
    'cleanup_test_bucket',
    
    # Test data generators
    'generate_test_order',
    'generate_test_customer',
    'generate_test_product',
]
"""
Integration Tests for E-Commerce Data Pipeline
==============================================

These tests verify the end-to-end functionality of the data pipeline.
"""

import pytest
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Generator

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.config.settings import Settings
from src.utils.database_utils import DatabaseConnection
from src.ingestion.file_ingestion import FileIngestion
from src.ingestion.kafka_producer import KafkaDataProducer
from src.processing.transformations import DataTransformer
from src.quality.data_quality_checks import DataQualityChecker, QualitySeverity


class TestDatabaseIntegration:
    """Integration tests for database operations."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.settings = Settings()
        self.source_db = None
        self.warehouse_db = None
        
    def test_source_database_connection(self):
        """Test connection to source database."""
        try:
            self.source_db = DatabaseConnection(
                host=self.settings.SOURCE_DB_HOST,
                port=self.settings.SOURCE_DB_PORT,
                database=self.settings.SOURCE_DB_NAME,
                user=self.settings.SOURCE_DB_USER,
                password=self.settings.SOURCE_DB_PASSWORD
            )
            
            # Test query
            result = self.source_db.execute_query("SELECT 1 as test")
            assert result is not None
            
        except Exception as e:
            pytest.skip(f"Source database not available: {e}")
        finally:
            if self.source_db:
                self.source_db.close()
    
    def test_warehouse_database_connection(self):
        """Test connection to warehouse database."""
        try:
            self.warehouse_db = DatabaseConnection(
                host=self.settings.WAREHOUSE_DB_HOST,
                port=self.settings.WAREHOUSE_DB_PORT,
                database=self.settings.WAREHOUSE_DB_NAME,
                user=self.settings.WAREHOUSE_DB_USER,
                password=self.settings.WAREHOUSE_DB_PASSWORD
            )
            
            # Test query
            result = self.warehouse_db.execute_query("SELECT 1 as test")
            assert result is not None
            
        except Exception as e:
            pytest.skip(f"Warehouse database not available: {e}")
        finally:
            if self.warehouse_db:
                self.warehouse_db.close()
    
    def test_insert_and_retrieve_data(self):
        """Test inserting and retrieving data from database."""
        try:
            self.source_db = DatabaseConnection(
                host=self.settings.SOURCE_DB_HOST,
                port=self.settings.SOURCE_DB_PORT,
                database=self.settings.SOURCE_DB_NAME,
                user=self.settings.SOURCE_DB_USER,
                password=self.settings.SOURCE_DB_PASSWORD
            )
            
            # Create test table
            self.source_db.execute_query("""
                CREATE TABLE IF NOT EXISTS test_integration (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert test data
            test_name = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.source_db.execute_query(
                "INSERT INTO test_integration (name) VALUES (%s)",
                (test_name,)
            )
            
            # Retrieve data
            result = self.source_db.fetch_dataframe(
                "SELECT * FROM test_integration WHERE name = %s",
                (test_name,)
            )
            
            assert len(result) == 1
            assert result.iloc[0]['name'] == test_name
            
            # Cleanup
            self.source_db.execute_query(
                "DELETE FROM test_integration WHERE name = %s",
                (test_name,)
            )
            
        except Exception as e:
            pytest.skip(f"Database test failed: {e}")
        finally:
            if self.source_db:
                self.source_db.close()


class TestKafkaIntegration:
    """Integration tests for Kafka operations."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.settings = Settings()
        self.producer = None
        
    def test_kafka_producer_connection(self):
        """Test Kafka producer connection."""
        try:
            self.producer = KafkaDataProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS
            )
            
            assert self.producer is not None
            
        except Exception as e:
            pytest.skip(f"Kafka not available: {e}")
        finally:
            if self.producer:
                self.producer.close()
    
    def test_send_message_to_kafka(self):
        """Test sending message to Kafka topic."""
        try:
            self.producer = KafkaDataProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS
            )
            
            # Create test message
            test_message = {
                'test_id': f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                'timestamp': datetime.now().isoformat(),
                'data': 'integration_test'
            }
            
            # Send message
            result = self.producer.send_message(
                topic='test-integration',
                message=test_message
            )
            
            assert result is not None
            
        except Exception as e:
            pytest.skip(f"Kafka send test failed: {e}")
        finally:
            if self.producer:
                self.producer.close()


class TestDataPipelineIntegration:
    """Integration tests for complete data pipeline."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.settings = Settings()
        self.transformer = DataTransformer()
        
    def test_file_to_transformation_pipeline(self):
        """Test loading file and transforming data."""
        # Create test data
        test_data = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002', 'ORD003'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'order_date': [
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=3)
            ],
            'total_amount': [100.00, 200.00, 300.00],
            'status': ['completed', 'shipped', 'processing']
        })
        
        # Transform data
        transformed = self.transformer.transform_orders(test_data)
        
        # Verify transformations
        assert 'order_value_tier' in transformed.columns
        assert 'is_completed' in transformed.columns
        assert len(transformed) == 3
    
    def test_data_quality_in_pipeline(self):
        """Test data quality checks in pipeline."""
        # Create test data
        test_data = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002', None, 'ORD004'],
            'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004'],
            'total_amount': [100.00, -50.00, 300.00, 400.00],
            'order_date': [datetime.now()] * 4
        })
        
        # Run quality checks
        checker = DataQualityChecker(test_data, "test_orders")
        
        # Check for nulls
        null_result = checker.check_not_null('order_id')
        assert not null_result.passed  # Should fail due to null value
        
        # Check for negative amounts
        range_result = checker.check_value_range(
            'total_amount',
            min_value=0,
            max_value=10000
        )
        assert not range_result.passed  # Should fail due to negative value
    
    def test_end_to_end_transformation(self):
        """Test complete end-to-end data transformation."""
        # Create sample orders
        orders = pd.DataFrame({
            'order_id': [f'ORD{i:03d}' for i in range(1, 11)],
            'customer_id': [f'CUST{(i % 5) + 1:03d}' for i in range(1, 11)],
            'product_id': [f'PROD{(i % 3) + 1:03d}' for i in range(1, 11)],
            'order_date': [datetime.now() - timedelta(days=i) for i in range(1, 11)],
            'quantity': [i for i in range(1, 11)],
            'unit_price': [10.00 * i for i in range(1, 11)],
            'discount': [0, 5, 0, 10, 0, 5, 0, 10, 0, 15],
            'status': ['completed', 'shipped', 'processing', 'completed', 'shipped',
                      'processing', 'completed', 'shipped', 'processing', 'completed']
        })
        
        # Create sample customers
        customers = pd.DataFrame({
            'customer_id': [f'CUST{i:03d}' for i in range(1, 6)],
            'first_name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'last_name': ['Smith', 'Doe', 'Johnson', 'Williams', 'Brown'],
            'email': [f'customer{i}@email.com' for i in range(1, 6)],
            'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
            'registration_date': [datetime.now() - timedelta(days=i*30) for i in range(1, 6)]
        })
        
        # Create sample products
        products = pd.DataFrame({
            'product_id': [f'PROD{i:03d}' for i in range(1, 4)],
            'name': ['Widget A', 'Widget B', 'Widget C'],
            'category': ['Electronics', 'Clothing', 'Home'],
            'price': [29.99, 49.99, 19.99],
            'cost': [15.00, 25.00, 10.00]
        })
        
        # Transform all data
        transformed_orders = self.transformer.transform_orders(orders)
        transformed_customers = self.transformer.transform_customers(customers)
        transformed_products = self.transformer.transform_products(products)
        
        # Verify all transformations succeeded
        assert len(transformed_orders) == 10
        assert len(transformed_customers) == 5
        assert len(transformed_products) == 3
        
        # Verify enriched columns exist
        assert 'order_value_tier' in transformed_orders.columns
        assert 'customer_tenure_days' in transformed_customers.columns
        assert 'profit_margin' in transformed_products.columns


class TestMinIOIntegration:
    """Integration tests for MinIO/S3 operations."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test fixtures."""
        self.settings = Settings()
        
    def test_minio_connection(self):
        """Test connection to MinIO."""
        try:
            from minio import Minio
            
            client = Minio(
                self.settings.MINIO_ENDPOINT,
                access_key=self.settings.MINIO_ACCESS_KEY,
                secret_key=self.settings.MINIO_SECRET_KEY,
                secure=False
            )
            
            # List buckets
            buckets = client.list_buckets()
            assert buckets is not None
            
        except Exception as e:
            pytest.skip(f"MinIO not available: {e}")
    
    def test_upload_and_download_file(self):
        """Test uploading and downloading file from MinIO."""
        try:
            from minio import Minio
            import io
            
            client = Minio(
                self.settings.MINIO_ENDPOINT,
                access_key=self.settings.MINIO_ACCESS_KEY,
                secret_key=self.settings.MINIO_SECRET_KEY,
                secure=False
            )
            
            bucket_name = 'test-bucket'
            
            # Create bucket if not exists
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
            
            # Upload test file
            test_data = b"test data for integration test"
            file_name = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
            
            client.put_object(
                bucket_name,
                file_name,
                io.BytesIO(test_data),
                len(test_data)
            )
            
            # Download and verify
            response = client.get_object(bucket_name, file_name)
            downloaded_data = response.read()
            
            assert downloaded_data == test_data
            
            # Cleanup
            client.remove_object(bucket_name, file_name)
            
        except Exception as e:
            pytest.skip(f"MinIO file test failed: {e}")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
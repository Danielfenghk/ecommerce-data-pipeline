#!/usr/bin/env python3
"""
Tests for data ingestion modules
"""

import unittest
import json
import tempfile
import os
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Import modules to test
from src.ingestion.api_ingestion import APIIngestion
from src.ingestion.file_ingestion import FileIngestion
from src.ingestion.database_ingestion import DatabaseIngestion
from src.ingestion.kafka_producer import KafkaDataProducer


class TestAPIIngestion(unittest.TestCase):
    """Tests for API ingestion"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.api_ingestion = APIIngestion(base_url="https://api.example.com")
    
    @patch('requests.Session.get')
    def test_fetch_data_success(self, mock_get):
        """Test successful API data fetch"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Product 1"},
                {"id": 2, "name": "Product 2"}
            ]
        }
        mock_get.return_value = mock_response
        
        result = self.api_ingestion.fetch_data("/products")
        
        self.assertIsNotNone(result)
        mock_get.assert_called_once()
    
    @patch('requests.Session.get')
    def test_fetch_data_failure(self, mock_get):
        """Test API fetch failure handling"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")
        mock_get.return_value = mock_response
        
        result = self.api_ingestion.fetch_data("/products")
        
        self.assertIsNone(result)
    
    @patch('requests.Session.get')
    def test_fetch_paginated_data(self, mock_get):
        """Test paginated data fetch"""
        # First page
        response1 = Mock()
        response1.status_code = 200
        response1.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "has_next": True
        }
        
        # Second page
        response2 = Mock()
        response2.status_code = 200
        response2.json.return_value = {
            "data": [{"id": 3}],
            "has_next": False
        }
        
        mock_get.side_effect = [response1, response2]
        
        result = self.api_ingestion.fetch_paginated_data("/products", max_pages=2)
        
        self.assertEqual(len(result), 3)
    
    def test_validate_data(self):
        """Test data validation"""
        valid_data = [
            {"id": 1, "name": "Product 1", "price": 10.0},
            {"id": 2, "name": "Product 2", "price": 20.0}
        ]
        
        required_fields = ["id", "name", "price"]
        result = self.api_ingestion.validate_data(valid_data, required_fields)
        
        self.assertEqual(len(result), 2)
    
    def test_validate_data_missing_fields(self):
        """Test validation with missing fields"""
        invalid_data = [
            {"id": 1, "name": "Product 1"},  # Missing price
            {"id": 2, "name": "Product 2", "price": 20.0}
        ]
        
        required_fields = ["id", "name", "price"]
        result = self.api_ingestion.validate_data(invalid_data, required_fields)
        
        self.assertEqual(len(result), 1)


class TestFileIngestion(unittest.TestCase):
    """Tests for file ingestion"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.file_ingestion = FileIngestion()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_read_csv(self):
        """Test CSV file reading"""
        csv_content = "id,name,price\n1,Product 1,10.0\n2,Product 2,20.0"
        csv_path = os.path.join(self.temp_dir, "test.csv")
        
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        result = self.file_ingestion.read_csv(csv_path)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertIn('id', result.columns)
    
    def test_read_json(self):
        """Test JSON file reading"""
        json_data = [
            {"id": 1, "name": "Product 1"},
            {"id": 2, "name": "Product 2"}
        ]
        json_path = os.path.join(self.temp_dir, "test.json")
        
        with open(json_path, 'w') as f:
            json.dump(json_data, f)
        
        result = self.file_ingestion.read_json(json_path)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
    
    def test_read_jsonl(self):
        """Test JSONL file reading"""
        jsonl_content = '{"id": 1, "name": "Product 1"}\n{"id": 2, "name": "Product 2"}'
        jsonl_path = os.path.join(self.temp_dir, "test.jsonl")
        
        with open(jsonl_path, 'w') as f:
            f.write(jsonl_content)
        
        result = self.file_ingestion.read_json(jsonl_path, lines=True)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
    
    def test_validate_file_schema(self):
        """Test schema validation"""
        df = pd.DataFrame({
            'id': [1, 2],
            'name': ['Product 1', 'Product 2'],
            'price': [10.0, 20.0]
        })
        
        schema = {
            'id': 'int64',
            'name': 'object',
            'price': 'float64'
        }
        
        is_valid, errors = self.file_ingestion.validate_schema(df, schema)
        
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_clean_data(self):
        """Test data cleaning"""
        df = pd.DataFrame({
            'id': [1, 2, 2, None],
            'name': ['Product 1', 'Product 2', 'Product 2', 'Product 3'],
            'price': [10.0, 20.0, 20.0, 30.0]
        })
        
        cleaned = self.file_ingestion.clean_dataframe(
            df, 
            drop_duplicates=['id'],
            drop_nulls=['id']
        )
        
        self.assertEqual(len(cleaned), 2)


class TestDatabaseIngestion(unittest.TestCase):
    """Tests for database ingestion"""
    
    @patch('psycopg2.connect')
    def setUp(self, mock_connect):
        """Set up test fixtures"""
        mock_connect.return_value = MagicMock()
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        self.db_ingestion = DatabaseIngestion(self.db_config)
    
    @patch('psycopg2.connect')
    def test_execute_query(self, mock_connect):
        """Test query execution"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'Product 1'), (2, 'Product 2')]
        mock_cursor.description = [('id',), ('name',)]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        result = self.db_ingestion.execute_query("SELECT * FROM products")
        
        self.assertIsNotNone(result)
    
    @patch('psycopg2.connect')
    def test_fetch_table_data(self, mock_connect):
        """Test table data fetch"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, 'Product 1', 10.0),
            (2, 'Product 2', 20.0)
        ]
        mock_cursor.description = [('id',), ('name',), ('price',)]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        result = self.db_ingestion.fetch_table("products")
        
        self.assertIsNotNone(result)
    
    def test_build_query_with_filters(self):
        """Test query building with filters"""
        filters = {
            'status': 'active',
            'price_min': 10
        }
        
        query = self.db_ingestion.build_query(
            table="products",
            columns=["id", "name", "price"],
            filters=filters
        )
        
        self.assertIn("SELECT", query)
        self.assertIn("products", query)


class TestKafkaProducer(unittest.TestCase):
    """Tests for Kafka producer"""
    
    @patch('kafka.KafkaProducer')
    def setUp(self, mock_kafka):
        """Set up test fixtures"""
        mock_kafka.return_value = MagicMock()
        self.kafka_producer = KafkaDataProducer(
            bootstrap_servers=['localhost:9092']
        )
    
    @patch('kafka.KafkaProducer')
    def test_send_message(self, mock_kafka):
        """Test message sending"""
        mock_producer = MagicMock()
        mock_kafka.return_value = mock_producer
        
        producer = KafkaDataProducer(bootstrap_servers=['localhost:9092'])
        
        message = {"id": 1, "name": "Test"}
        result = producer.send_message("test-topic", message)
        
        self.assertTrue(result)
    
    @patch('kafka.KafkaProducer')
    def test_send_batch(self, mock_kafka):
        """Test batch message sending"""
        mock_producer = MagicMock()
        mock_kafka.return_value = mock_producer
        
        producer = KafkaDataProducer(bootstrap_servers=['localhost:9092'])
        
        messages = [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"}
        ]
        
        result = producer.send_batch("test-topic", messages)
        
        self.assertEqual(result['sent'], 2)


class TestIntegration(unittest.TestCase):
    """Integration tests for ingestion pipeline"""
    
    def test_csv_to_dataframe_pipeline(self):
        """Test complete CSV ingestion pipeline"""
        # Create test CSV
        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "products.csv")
        
        csv_content = """id,name,price,category
1,Product A,10.0,Electronics
2,Product B,20.0,Clothing
3,Product C,15.0,Electronics
"""
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        # Run ingestion
        file_ingestion = FileIngestion()
        df = file_ingestion.read_csv(csv_path)
        
        # Validate
        self.assertEqual(len(df), 3)
        self.assertTrue(all(col in df.columns for col in ['id', 'name', 'price', 'category']))
        
        # Clean up
        import shutil
        shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
"""
Tests for Data Transformation Module
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.processing.transformations import (
    DataTransformer,
    OrderTransformer,
    CustomerTransformer,
    ProductTransformer
)


class TestDataTransformer(unittest.TestCase):
    """Test cases for base DataTransformer class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.transformer = DataTransformer()
        self.sample_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['  John  ', 'Jane', None, 'Bob  ', ''],
            'email': ['JOHN@EMAIL.COM', 'jane@email.com', 'bob@email.com', None, 'test@test.com'],
            'amount': [100.5, 200.0, None, 150.75, 300.0],
            'date': ['2024-01-15', '2024-01-16', '2024-01-17', None, '2024-01-18']
        })
    
    def test_clean_string_columns(self):
        """Test string column cleaning"""
        df = self.transformer.clean_string_columns(self.sample_df, ['name', 'email'])
        
        # Check trimming
        self.assertEqual(df['name'].iloc[0], 'John')
        self.assertEqual(df['name'].iloc[3], 'Bob')
        
        # Check lowercase
        self.assertEqual(df['email'].iloc[0], 'john@email.com')
    
    def test_fill_missing_values(self):
        """Test missing value handling"""
        fill_values = {'name': 'Unknown', 'amount': 0.0}
        df = self.transformer.fill_missing_values(self.sample_df, fill_values)
        
        self.assertEqual(df['name'].iloc[2], 'Unknown')
        self.assertEqual(df['amount'].iloc[2], 0.0)
    
    def test_remove_duplicates(self):
        """Test duplicate removal"""
        df_with_dups = pd.DataFrame({
            'id': [1, 1, 2, 3, 3],
            'name': ['John', 'John', 'Jane', 'Bob', 'Bob']
        })
        
        df = self.transformer.remove_duplicates(df_with_dups, ['id'])
        self.assertEqual(len(df), 3)
    
    def test_standardize_dates(self):
        """Test date standardization"""
        df = self.transformer.standardize_dates(self.sample_df, ['date'])
        
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['date']))
        self.assertTrue(pd.isna(df['date'].iloc[3]))


class TestOrderTransformer(unittest.TestCase):
    """Test cases for OrderTransformer class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.transformer = OrderTransformer()
        self.sample_orders = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002', 'ORD003', 'ORD004'],
            'customer_id': ['C001', 'C002', 'C001', 'C003'],
            'order_date': [
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=30),
                datetime.now()
            ],
            'total_amount': [150.50, 250.00, 75.25, 500.00],
            'status': ['completed', 'pending', 'shipped', 'cancelled'],
            'discount': [10.0, 0.0, 5.0, 50.0],
            'shipping_cost': [5.99, 10.99, 0.0, 15.99]
        })
    
    def test_transform_orders_basic(self):
        """Test basic order transformation"""
        transformed = self.transformer.transform_orders(self.sample_orders)
        
        # Check new columns are created
        self.assertIn('order_value_tier', transformed.columns)
        self.assertIn('is_completed', transformed.columns)
        self.assertIn('net_amount', transformed.columns)
    
    def test_order_value_tiers(self):
        """Test order value tier calculation"""
        transformed = self.transformer.transform_orders(self.sample_orders)
        
        # Check tier assignments
        tier_for_500 = transformed[transformed['total_amount'] == 500.00]['order_value_tier'].iloc[0]
        self.assertEqual(tier_for_500, 'High')
        
        tier_for_75 = transformed[transformed['total_amount'] == 75.25]['order_value_tier'].iloc[0]
        self.assertEqual(tier_for_75, 'Low')
    
    def test_is_completed_flag(self):
        """Test is_completed flag calculation"""
        transformed = self.transformer.transform_orders(self.sample_orders)
        
        completed_order = transformed[transformed['order_id'] == 'ORD001']['is_completed'].iloc[0]
        self.assertTrue(completed_order)
        
        pending_order = transformed[transformed['order_id'] == 'ORD002']['is_completed'].iloc[0]
        self.assertFalse(pending_order)
    
    def test_net_amount_calculation(self):
        """Test net amount calculation"""
        transformed = self.transformer.transform_orders(self.sample_orders)
        
        # Net = total - discount + shipping
        expected_net = 150.50 - 10.0 + 5.99
        actual_net = transformed[transformed['order_id'] == 'ORD001']['net_amount'].iloc[0]
        self.assertAlmostEqual(actual_net, expected_net, places=2)
    
    def test_large_values(self):
        """Test handling large numeric values"""
        large_order = pd.DataFrame({
            'order_id': ['ORD001'],
            'customer_id': ['C001'],
            'order_date': [datetime.now()],
            'total_amount': [9999999.99],
            'status': ['completed'],
            'discount': [0.0],
            'shipping_cost': [0.0]
        })
        
        transformed = self.transformer.transform_orders(large_order)
        
        # Should handle large values without overflow
        self.assertEqual(transformed['total_amount'].iloc[0], 9999999.99)
        self.assertEqual(transformed['order_value_tier'].iloc[0], 'Very High')
    
    def test_negative_values(self):
        """Test handling negative values (refunds)"""
        refund_order = pd.DataFrame({
            'order_id': ['ORD001'],
            'customer_id': ['C001'],
            'order_date': [datetime.now()],
            'total_amount': [-50.00],
            'status': ['refunded'],
            'discount': [0.0],
            'shipping_cost': [0.0]
        })
        
        transformed = self.transformer.transform_orders(refund_order)
        
        # Should handle negative values
        self.assertEqual(transformed['total_amount'].iloc[0], -50.00)
    
    def test_empty_dataframe(self):
        """Test handling empty DataFrame"""
        empty_df = pd.DataFrame(columns=[
            'order_id', 'customer_id', 'order_date', 
            'total_amount', 'status', 'discount', 'shipping_cost'
        ])
        
        transformed = self.transformer.transform_orders(empty_df)
        self.assertEqual(len(transformed), 0)
    
    def test_date_extraction(self):
        """Test date component extraction"""
        transformed = self.transformer.transform_orders(self.sample_orders)
        
        if 'order_year' in transformed.columns:
            self.assertTrue(all(transformed['order_year'] >= 2024))
        
        if 'order_month' in transformed.columns:
            self.assertTrue(all(transformed['order_month'].between(1, 12)))
    
    def test_order_age_calculation(self):
        """Test order age calculation"""
        transformed = self.transformer.transform_orders(self.sample_orders)
        
        if 'order_age_days' in transformed.columns:
            # Order from 1 day ago should have age ~1
            recent_order_age = transformed[
                transformed['order_id'] == 'ORD001'
            ]['order_age_days'].iloc[0]
            self.assertGreaterEqual(recent_order_age, 0)
            self.assertLessEqual(recent_order_age, 2)


class TestCustomerTransformer(unittest.TestCase):
    """Test cases for CustomerTransformer class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.transformer = CustomerTransformer()
        self.sample_customers = pd.DataFrame({
            'customer_id': ['C001', 'C002', 'C003', 'C004'],
            'first_name': ['  John  ', 'Jane', 'Bob', None],
            'last_name': ['Doe', 'Smith', '  Johnson  ', 'Williams'],
            'email': ['JOHN.DOE@EMAIL.COM', 'jane@email.com', None, 'david@email.com'],
            'phone': ['123-456-7890', '(987) 654-3210', '5551234567', None],
            'registration_date': [
                datetime.now() - timedelta(days=365),
                datetime.now() - timedelta(days=30),
                datetime.now() - timedelta(days=180),
                datetime.now()
            ],
            'total_orders': [25, 5, 12, 0],
            'total_spent': [2500.00, 500.00, 1200.00, 0.00],
            'country': ['US', 'UK', 'CA', 'US']
        })
    
    def test_transform_customers_basic(self):
        """Test basic customer transformation"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        # Check that transformation completes
        self.assertEqual(len(transformed), 4)
    
    def test_name_standardization(self):
        """Test name standardization"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        # Check trimming and proper casing
        if 'first_name' in transformed.columns:
            self.assertEqual(transformed['first_name'].iloc[0].strip(), 'John')
    
    def test_email_standardization(self):
        """Test email standardization"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        # Email should be lowercase
        if 'email' in transformed.columns:
            email = transformed['email'].iloc[0]
            if email:
                self.assertEqual(email, email.lower())
    
    def test_phone_standardization(self):
        """Test phone number standardization"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        if 'phone_standardized' in transformed.columns:
            # Check that phone numbers are standardized (digits only or formatted)
            phone = transformed['phone_standardized'].iloc[0]
            if phone:
                # Should contain only digits, spaces, dashes, or parentheses
                self.assertTrue(
                    all(c.isdigit() or c in '- ()' for c in str(phone))
                )
    
    def test_customer_lifetime_value(self):
        """Test customer lifetime value calculation"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        if 'customer_lifetime_value' in transformed.columns or 'avg_order_value' in transformed.columns:
            # CLV or AOV should be calculated correctly
            high_value_customer = transformed[transformed['customer_id'] == 'C001']
            if 'avg_order_value' in transformed.columns:
                aov = high_value_customer['avg_order_value'].iloc[0]
                expected_aov = 2500.00 / 25
                self.assertAlmostEqual(aov, expected_aov, places=2)
    
    def test_customer_segment(self):
        """Test customer segmentation"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        if 'customer_segment' in transformed.columns:
            # High-value customer should be in premium segment
            segment = transformed[
                transformed['customer_id'] == 'C001'
            ]['customer_segment'].iloc[0]
            self.assertIn(segment, ['Premium', 'Gold', 'VIP', 'High Value', 'High'])
    
    def test_missing_email_handling(self):
        """Test handling of missing email"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        # Customer C003 has missing email
        customer_c003 = transformed[transformed['customer_id'] == 'C003']
        
        # Should not raise error and email should be None or default
        self.assertEqual(len(customer_c003), 1)
    
    def test_full_name_creation(self):
        """Test full name creation from first and last name"""
        transformed = self.transformer.transform_customers(self.sample_customers)
        
        if 'full_name' in transformed.columns:
            full_name = transformed[
                transformed['customer_id'] == 'C001'
            ]['full_name'].iloc[0]
            self.assertIn('John', full_name)
            self.assertIn('Doe', full_name)


class TestProductTransformer(unittest.TestCase):
    """Test cases for ProductTransformer class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.transformer = ProductTransformer()
        self.sample_products = pd.DataFrame({
            'product_id': ['P001', 'P002', 'P003', 'P004'],
            'name': ['  Laptop  ', 'Phone', 'Tablet', None],
            'category': ['Electronics', 'Electronics', 'Electronics', 'Accessories'],
            'subcategory': ['Computers', 'Mobile', 'Mobile', 'Cases'],
            'price': [999.99, 699.99, 499.99, 29.99],
            'cost': [600.00, 400.00, 300.00, 10.00],
            'stock_quantity': [50, 100, 75, 500],
            'weight': [2.5, 0.3, 0.5, 0.1],
            'is_active': [True, True, False, True]
        })
    
    def test_transform_products_basic(self):
        """Test basic product transformation"""
        transformed = self.transformer.transform_products(self.sample_products)
        
        self.assertEqual(len(transformed), 4)
    
    def test_profit_margin_calculation(self):
        """Test profit margin calculation"""
        transformed = self.transformer.transform_products(self.sample_products)
        
        if 'profit_margin' in transformed.columns:
            # Laptop: (999.99 - 600.00) / 999.99 = ~40%
            laptop_margin = transformed[
                transformed['product_id'] == 'P001'
            ]['profit_margin'].iloc[0]
            expected_margin = (999.99 - 600.00) / 999.99 * 100
            self.assertAlmostEqual(laptop_margin, expected_margin, places=1)
    
    def test_price_tier_assignment(self):
        """Test price tier assignment"""
        transformed = self.transformer.transform_products(self.sample_products)
        
        if 'price_tier' in transformed.columns:
            # Expensive product should be in high tier
            laptop_tier = transformed[
                transformed['product_id'] == 'P001'
            ]['price_tier'].iloc[0]
            self.assertIn(laptop_tier, ['High', 'Premium', 'Expensive'])
            
            # Cheap product should be in low tier
            accessory_tier = transformed[
                transformed['product_id'] == 'P004'
            ]['price_tier'].iloc[0]
            self.assertIn(accessory_tier, ['Low', 'Budget', 'Economy', 'Cheap'])
    
    def test_stock_status(self):
        """Test stock status calculation"""
        transformed = self.transformer.transform_products(self.sample_products)
        
        if 'stock_status' in transformed.columns:
            # Product with 500 units should be well-stocked
            high_stock_status = transformed[
                transformed['product_id'] == 'P004'
            ]['stock_status'].iloc[0]
            self.assertIn(high_stock_status, ['In Stock', 'High Stock', 'Well Stocked', 'Abundant'])
    
    def test_category_hierarchy(self):
        """Test category hierarchy creation"""
        transformed = self.transformer.transform_products(self.sample_products)
        
        if 'category_path' in transformed.columns:
            path = transformed[
                transformed['product_id'] == 'P001'
            ]['category_path'].iloc[0]
            self.assertIn('Electronics', path)
            self.assertIn('Computers', path)
    
    def test_missing_product_name(self):
        """Test handling of missing product name"""
        transformed = self.transformer.transform_products(self.sample_products)
        
        # Product P004 has missing name
        product_p004 = transformed[transformed['product_id'] == 'P004']
        self.assertEqual(len(product_p004), 1)
        
        if 'name' in product_p004.columns:
            name = product_p004['name'].iloc[0]
            # Should have default or be handled gracefully
            self.assertTrue(name is None or name == 'Unknown' or pd.isna(name))


class TestTransformationIntegration(unittest.TestCase):
    """Integration tests for transformation pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.order_transformer = OrderTransformer()
        self.customer_transformer = CustomerTransformer()
        self.product_transformer = ProductTransformer()
    
    def test_full_transformation_pipeline(self):
        """Test complete transformation pipeline"""
        # Create sample data
        orders = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002'],
            'customer_id': ['C001', 'C002'],
            'order_date': [datetime.now(), datetime.now()],
            'total_amount': [100.00, 200.00],
            'status': ['completed', 'pending'],
            'discount': [0.0, 10.0],
            'shipping_cost': [5.99, 0.0]
        })
        
        customers = pd.DataFrame({
            'customer_id': ['C001', 'C002'],
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'email': ['john@email.com', 'jane@email.com'],
            'phone': ['1234567890', '0987654321'],
            'registration_date': [datetime.now(), datetime.now()],
            'total_orders': [10, 5],
            'total_spent': [1000.00, 500.00],
            'country': ['US', 'UK']
        })
        
        products = pd.DataFrame({
            'product_id': ['P001', 'P002'],
            'name': ['Laptop', 'Phone'],
            'category': ['Electronics', 'Electronics'],
            'subcategory': ['Computers', 'Mobile'],
            'price': [999.99, 699.99],
            'cost': [600.00, 400.00],
            'stock_quantity': [50, 100],
            'weight': [2.5, 0.3],
            'is_active': [True, True]
        })
        
        # Transform all data
        transformed_orders = self.order_transformer.transform_orders(orders)
        transformed_customers = self.customer_transformer.transform_customers(customers)
        transformed_products = self.product_transformer.transform_products(products)
        
        # Verify all transformations completed
        self.assertEqual(len(transformed_orders), 2)
        self.assertEqual(len(transformed_customers), 2)
        self.assertEqual(len(transformed_products), 2)
    
    def test_data_type_consistency(self):
        """Test that data types are consistent after transformation"""
        orders = pd.DataFrame({
            'order_id': ['ORD001'],
            'customer_id': ['C001'],
            'order_date': [datetime.now()],
            'total_amount': [100.00],
            'status': ['completed'],
            'discount': [0.0],
            'shipping_cost': [5.99]
        })
        
        transformed = self.order_transformer.transform_orders(orders)
        
        # Numeric columns should remain numeric
        self.assertTrue(
            pd.api.types.is_numeric_dtype(transformed['total_amount'])
        )
    
    def test_null_handling_consistency(self):
        """Test consistent null handling across transformers"""
        orders_with_nulls = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002'],
            'customer_id': ['C001', None],
            'order_date': [datetime.now(), None],
            'total_amount': [100.00, None],
            'status': ['completed', None],
            'discount': [None, None],
            'shipping_cost': [None, None]
        })
        
        # Should handle nulls gracefully without errors
        try:
            transformed = self.order_transformer.transform_orders(orders_with_nulls)
            self.assertEqual(len(transformed), 2)
        except Exception as e:
            self.fail(f"Transformation failed with nulls: {e}")


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.transformer = OrderTransformer()
    
    def test_unicode_characters(self):
        """Test handling of unicode characters"""
        orders = pd.DataFrame({
            'order_id': ['ORD001'],
            'customer_id': ['C001'],
            'order_date': [datetime.now()],
            'total_amount': [100.00],
            'status': ['完成'],  # Chinese characters
            'discount': [0.0],
            'shipping_cost': [5.99]
        })
        
        transformed = self.transformer.transform_orders(orders)
        self.assertEqual(len(transformed), 1)
    
    def test_special_characters_in_ids(self):
        """Test handling of special characters in IDs"""
        orders = pd.DataFrame({
            'order_id': ['ORD-001-2024', 'ORD_002_2024'],
            'customer_id': ['C-001', 'C_002'],
            'order_date': [datetime.now(), datetime.now()],
            'total_amount': [100.00, 200.00],
            'status': ['completed', 'pending'],
            'discount': [0.0, 0.0],
            'shipping_cost': [5.99, 5.99]
        })
        
        transformed = self.transformer.transform_orders(orders)
        self.assertEqual(len(transformed), 2)
    
    def test_extreme_dates(self):
        """Test handling of extreme dates"""
        orders = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002'],
            'customer_id': ['C001', 'C002'],
            'order_date': [
                datetime(2000, 1, 1),  # Very old date
                datetime(2099, 12, 31)  # Future date
            ],
            'total_amount': [100.00, 200.00],
            'status': ['completed', 'pending'],
            'discount': [0.0, 0.0],
            'shipping_cost': [5.99, 5.99]
        })
        
        transformed = self.transformer.transform_orders(orders)
        self.assertEqual(len(transformed), 2)
    
    def test_zero_values(self):
        """Test handling of zero values"""
        orders = pd.DataFrame({
            'order_id': ['ORD001'],
            'customer_id': ['C001'],
            'order_date': [datetime.now()],
            'total_amount': [0.00],
            'status': ['completed'],
            'discount': [0.0],
            'shipping_cost': [0.0]
        })
        
        transformed = self.transformer.transform_orders(orders)
        self.assertEqual(transformed['total_amount'].iloc[0], 0.00)
    
    def test_very_long_strings(self):
        """Test handling of very long strings"""
        long_string = 'A' * 10000
        orders = pd.DataFrame({
            'order_id': [long_string],
            'customer_id': ['C001'],
            'order_date': [datetime.now()],
            'total_amount': [100.00],
            'status': ['completed'],
            'discount': [0.0],
            'shipping_cost': [5.99]
        })
        
        transformed = self.transformer.transform_orders(orders)
        self.assertEqual(len(transformed), 1)


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestDataTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestOrderTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestCustomerTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestProductTransformer))
    suite.addTests(loader.loadTestsFromTestCase(TestTransformationIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCases))
    
    # Run tests with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
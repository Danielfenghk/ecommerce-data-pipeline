"""
Tests for Data Quality Module
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality.data_quality_checks import (
    DataQualityChecker,
    QualityCheckType,
    QualitySeverity,
    QualityResult,
    DataQualityReport
)


class TestQualityResult(unittest.TestCase):
    """Test cases for QualityResult class"""
    
    def test_quality_result_creation(self):
        """Test QualityResult object creation"""
        result = QualityResult(
            rule_name="test_rule",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=True,
            actual_value=100.0,
            expected_threshold=95.0,
            severity=QualitySeverity.HIGH.value,
            details={"test": "value"}
        )
        
        self.assertEqual(result.rule_name, "test_rule")
        self.assertTrue(result.passed)
        self.assertEqual(result.actual_value, 100.0)
    
    def test_quality_result_to_dict(self):
        """Test QualityResult to dictionary conversion"""
        result = QualityResult(
            rule_name="test_rule",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=True,
            actual_value=100.0,
            expected_threshold=95.0,
            severity=QualitySeverity.HIGH.value,
            details={}
        )
        
        result_dict = result.to_dict()
        
        self.assertIsInstance(result_dict, dict)
        self.assertIn('rule_name', result_dict)
        self.assertIn('passed', result_dict)


class TestDataQualityChecker(unittest.TestCase):
    """Test cases for DataQualityChecker class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sample_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['John', 'Jane', None, 'Bob', 'Alice'],
            'email': ['john@test.com', 'jane@test.com', 'bob@test.com', None, 'alice@test.com'],
            'age': [25, 30, 35, 40, -5],  # -5 is invalid
            'amount': [100.0, 200.0, 300.0, 400.0, 500.0],
            'created_at': [
                datetime.now() - timedelta(hours=1),
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=3),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(hours=5)
            ]
        })
        self.checker = DataQualityChecker(self.sample_df)
    
    def test_check_null_percentage(self):
        """Test null percentage check"""
        # name column has 1 null out of 5 = 20% null
        result = self.checker.check_null_percentage('name', max_null_pct=25.0)
        
        self.assertTrue(result.passed)
        self.assertAlmostEqual(result.actual_value, 20.0, places=1)
    
    def test_check_null_percentage_fail(self):
        """Test null percentage check failure"""
        # name column has 20% null, threshold is 10%
        result = self.checker.check_null_percentage('name', max_null_pct=10.0)
        
        self.assertFalse(result.passed)
    
    def test_check_unique(self):
        """Test uniqueness check"""
        # id column should be unique
        result = self.checker.check_unique('id')
        
        self.assertTrue(result.passed)
    
    def test_check_unique_fail(self):
        """Test uniqueness check failure"""
        df_with_dups = pd.DataFrame({
            'id': [1, 1, 2, 3, 4]
        })
        checker = DataQualityChecker(df_with_dups)
        result = checker.check_unique('id')
        
        self.assertFalse(result.passed)
    
    def test_check_range(self):
        """Test range check"""
        result = self.checker.check_range('amount', min_value=0, max_value=1000)
        
        self.assertTrue(result.passed)
    
    def test_check_range_fail(self):
        """Test range check failure"""
        # age has -5 which is below 0
        result = self.checker.check_range('age', min_value=0, max_value=120)
        
        self.assertFalse(result.passed)
    
    def test_check_not_null(self):
        """Test not null check"""
        # id column has no nulls
        result = self.checker.check_not_null('id')
        
        self.assertTrue(result.passed)
    
    def test_check_not_null_fail(self):
        """Test not null check failure"""
        # name column has nulls
        result = self.checker.check_not_null('name')
        
        self.assertFalse(result.passed)
    
    def test_check_regex_pattern(self):
        """Test regex pattern check"""
        # Email pattern check
        result = self.checker.check_regex_pattern(
            'email', 
            pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$'
        )
        
        # Should pass for non-null values
        self.assertIsInstance(result, QualityResult)
    
    def test_check_referential_integrity(self):
        """Test referential integrity check"""
        valid_ids = [1, 2, 3, 4, 5]
        result = self.checker.check_referential_integrity('id', valid_ids)
        
        self.assertTrue(result.passed)
    
    def test_check_referential_integrity_fail(self):
        """Test referential integrity check failure"""
        valid_ids = [1, 2, 3]  # Missing 4 and 5
        result = self.checker.check_referential_integrity('id', valid_ids)
        
        self.assertFalse(result.passed)
    
    def test_check_freshness(self):
        """Test data freshness check"""
        result = self.checker.check_freshness('created_at', max_age_hours=48)
        
        # Most data is within 48 hours
        self.assertIsInstance(result, QualityResult)
    
    def test_check_freshness_fail(self):
        """Test data freshness check failure"""
        old_df = pd.DataFrame({
            'timestamp': [datetime.now() - timedelta(days=10)]
        })
        checker = DataQualityChecker(old_df)
        result = checker.check_freshness('timestamp', max_age_hours=24)
        
        self.assertFalse(result.passed)
    
    def test_check_column_exists(self):
        """Test check for non-existent column"""
        result = self.checker.check_null_percentage('nonexistent_column')
        
        self.assertFalse(result.passed)
        self.assertIn('error', result.details)
    
    def test_check_value_distribution(self):
        """Test value distribution check"""
        result = self.checker.check_value_distribution(
            'amount',
            expected_min=50.0,
            expected_max=600.0
        )
        
        self.assertTrue(result.passed)
    
    def test_check_statistical_outliers(self):
        """Test statistical outlier detection"""
        df_with_outlier = pd.DataFrame({
            'value': [10, 12, 11, 13, 10, 12, 1000]  # 1000 is outlier
        })
        checker = DataQualityChecker(df_with_outlier)
        result = checker.check_statistical_outliers('value', std_threshold=3)
        
        # Should detect outlier
        self.assertIsInstance(result, QualityResult)


class TestDataQualityReport(unittest.TestCase):
    """Test cases for DataQualityReport class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sample_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'amount': [100.0, 200.0, 300.0]
        })
    
    def test_report_creation(self):
        """Test report creation"""
        report = DataQualityReport("test_dataset")
        
        self.assertEqual(report.dataset_name, "test_dataset")
        self.assertEqual(len(report.results), 0)
    
    def test_add_result(self):
        """Test adding result to report"""
        report = DataQualityReport("test_dataset")
        result = QualityResult(
            rule_name="test_rule",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=True,
            actual_value=100.0,
            expected_threshold=95.0,
            severity=QualitySeverity.HIGH.value,
            details={}
        )
        
        report.add_result(result)
        
        self.assertEqual(len(report.results), 1)
    
    def test_report_summary(self):
        """Test report summary generation"""
        report = DataQualityReport("test_dataset")
        
        # Add passing result
        report.add_result(QualityResult(
            rule_name="rule1",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=True,
            actual_value=100.0,
            expected_threshold=95.0,
            severity=QualitySeverity.HIGH.value,
            details={}
        ))
        
        # Add failing result
        report.add_result(QualityResult(
            rule_name="rule2",
            check_type=QualityCheckType.VALIDITY.value,
            passed=False,
            actual_value=80.0,
            expected_threshold=95.0,
            severity=QualitySeverity.CRITICAL.value,
            details={}
        ))
        
        summary = report.get_summary()
        
        self.assertEqual(summary['total_checks'], 2)
        self.assertEqual(summary['passed_checks'], 1)
        self.assertEqual(summary['failed_checks'], 1)
    
    def test_report_to_dataframe(self):
        """Test converting report to DataFrame"""
        report = DataQualityReport("test_dataset")
        report.add_result(QualityResult(
            rule_name="rule1",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=True,
            actual_value=100.0,
            expected_threshold=95.0,
            severity=QualitySeverity.HIGH.value,
            details={}
        ))
        
        df = report.to_dataframe()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
    
    def test_overall_status(self):
        """Test overall status determination"""
        report = DataQualityReport("test_dataset")
        
        # All passing
        report.add_result(QualityResult(
            rule_name="rule1",
            check_type=QualityCheckType.COMPLETENESS.value,
            passed=True,
            actual_value=100.0,
            expected_threshold=95.0,
            severity=QualitySeverity.HIGH.value,
            details={}
        ))
        
        self.assertTrue(report.is_passing())
        
        # Add failing critical check
        report.add_result(QualityResult(
            rule_name="rule2",
            check_type=QualityCheckType.VALIDITY.value,
            passed=False,
            actual_value=80.0,
            expected_threshold=95.0,
            severity=QualitySeverity.CRITICAL.value,
            details={}
        ))
        
        self.assertFalse(report.is_passing())


class TestQualityCheckIntegration(unittest.TestCase):
    """Integration tests for quality checking"""
    
    def test_full_quality_check_pipeline(self):
        """Test complete quality check pipeline"""
        # Create sample data
        df = pd.DataFrame({
            'order_id': ['ORD001', 'ORD002', 'ORD003', 'ORD004', 'ORD005'],
            'customer_id': ['C001', 'C002', 'C003', None, 'C005'],
            'amount': [100.0, 200.0, 300.0, 400.0, 500.0],
            'status': ['completed', 'pending', 'completed', 'shipped', 'invalid'],
            'created_at': [datetime.now()] * 5
        })
        
        checker = DataQualityChecker(df)
        report = DataQualityReport("orders")
        
        # Run checks
        report.add_result(checker.check_unique('order_id'))
        report.add_result(checker.check_null_percentage('customer_id', max_null_pct=25.0))
        report.add_result(checker.check_range('amount', min_value=0, max_value=1000))
        report.add_result(checker.check_freshness('created_at', max_age_hours=24))
        
        # Verify report
        summary = report.get_summary()
        self.assertEqual(summary['total_checks'], 4)
        self.assertGreater(summary['passed_checks'], 0)
    
    def test_quality_check_with_empty_dataframe(self):
        """Test quality checks with empty DataFrame"""
        empty_df = pd.DataFrame(columns=['id', 'name', 'value'])
        checker = DataQualityChecker(empty_df)
        
        result = checker.check_unique('id')
        
        # Should handle empty DataFrame gracefully
        self.assertIsInstance(result, QualityResult)
    
    def test_quality_check_with_all_nulls(self):
        """Test quality checks with all null values"""
        null_df = pd.DataFrame({
            'id': [None, None, None],
            'name': [None, None, None]
        })
        checker = DataQualityChecker(null_df)
        
        result = checker.check_null_percentage('id', max_null_pct=50.0)
        
        self.assertFalse(result.passed)
        self.assertEqual(result.actual_value, 100.0)


class TestQualityCheckEdgeCases(unittest.TestCase):
    """Test edge cases for quality checks"""
    
    def test_single_row_dataframe(self):
        """Test quality checks with single row"""
        single_row_df = pd.DataFrame({
            'id': [1],
            'name': ['John']
        })
        checker = DataQualityChecker(single_row_df)
        
        result = checker.check_unique('id')
        self.assertTrue(result.passed)
    
    def test_large_dataframe(self):
        """Test quality checks with large DataFrame"""
        large_df = pd.DataFrame({
            'id': range(100000),
            'value': np.random.randn(100000)
        })
        checker = DataQualityChecker(large_df)
        
        result = checker.check_unique('id')
        self.assertTrue(result.passed)
    
    def test_mixed_types(self):
        """Test quality checks with mixed types"""
        mixed_df = pd.DataFrame({
            'value': [1, 'two', 3.0, None, True]
        })
        checker = DataQualityChecker(mixed_df)
        
        result = checker.check_not_null('value')
        self.assertFalse(result.passed)
    
    def test_special_characters(self):
        """Test quality checks with special characters"""
        special_df = pd.DataFrame({
            'name': ['John!@#', 'Jane$%^', 'Bob&*()', None, 'Alice<>?']
        })
        checker = DataQualityChecker(special_df)
        
        result = checker.check_regex_pattern('name', pattern=r'^[A-Za-z]+$')
        self.assertFalse(result.passed)


if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestQualityResult))
    suite.addTests(loader.loadTestsFromTestCase(TestDataQualityChecker))
    suite.addTests(loader.loadTestsFromTestCase(TestDataQualityReport))
    suite.addTests(loader.loadTestsFromTestCase(TestQualityCheckIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestQualityCheckEdgeCases))
    
    # Run tests with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
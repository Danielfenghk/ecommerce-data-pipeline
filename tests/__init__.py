"""
Test Suite for E-Commerce Data Pipeline
=======================================

This package contains all tests for the data pipeline:

- Unit Tests: Test individual functions and classes
- Integration Tests: Test with real services
- Data Quality Tests: Validate data quality checks

Running Tests:
    # Run all tests
    pytest tests/ -v
    
    # Run unit tests only
    pytest tests/ -v --ignore=tests/integration
    
    # Run with coverage
    pytest tests/ -v --cov=src --cov-report=html
"""

import os
import sys

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
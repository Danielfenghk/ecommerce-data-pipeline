"""
Lakehouse Module
================

This module provides the Iceberg-based Data Lakehouse functionality
implementing the Medallion Architecture (Bronze → Silver → Gold).

Components:
- IcebergManager: Manages Iceberg catalog and table operations
- BronzeLayer: Raw data ingestion
- SilverLayer: Data cleaning and standardization
- GoldLayer: Business aggregations and analytics
"""

from .iceberg_manager import IcebergManager
from .bronze_layer import BronzeLayer
from .silver_layer import SilverLayer
from .gold_layer import GoldLayer

__all__ = [
    'IcebergManager',
    'BronzeLayer',
    'SilverLayer',
    'GoldLayer',
]

__version__ = '1.0.0'
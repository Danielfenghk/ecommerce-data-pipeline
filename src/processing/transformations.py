"""
Data Transformation Module
Provides transformation functions for e-commerce data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import hashlib
import logging
from functools import wraps
import time

logger = logging.getLogger(__name__)


def timing_decorator(func):
    """Decorator to measure function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper


class DataTransformer:
    """Base class for data transformations"""
    
    def __init__(self):
        self.transformation_log = []
    
    def log_transformation(self, name: str, input_rows: int, output_rows: int):
        """Log transformation details"""
        self.transformation_log.append({
            'transformation': name,
            'input_rows': input_rows,
            'output_rows': output_rows,
            'timestamp': datetime.now().isoformat(),
            'rows_affected': input_rows - output_rows
        })
    
    def get_transformation_summary(self) -> pd.DataFrame:
        """Get summary of all transformations"""
        return pd.DataFrame(self.transformation_log)


class OrderTransformer(DataTransformer):
    """Transformer for order data"""
    
    def __init__(self):
        super().__init__()
        self.currency_rates = {
            'USD': 1.0,
            'EUR': 1.1,
            'GBP': 1.27,
            'CAD': 0.74,
            'AUD': 0.65
        }
    
    @timing_decorator
    def transform_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main transformation function for orders
        
        Args:
            df: Raw orders DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Starting order transformation with {len(df)} records")
        input_rows = len(df)
        
        result = df.copy()
        
        # Clean and standardize columns
        result = self._standardize_columns(result)
        
        # Parse and validate dates
        result = self._parse_dates(result)
        
        # Calculate derived fields
        result = self._calculate_derived_fields(result)
        
        # Add time-based features
        result = self._add_time_features(result)
        
        # Categorize orders
        result = self._categorize_orders(result)
        
        # Handle currency conversion
        result = self._convert_currency(result)
        
        # Generate surrogate keys
        result = self._generate_keys(result)
        
        self.log_transformation('transform_orders', input_rows, len(result))
        logger.info(f"Order transformation complete: {len(result)} records")
        
        return result
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and types"""
        # Rename columns to snake_case
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        
        # Standardize string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col not in ['order_id', 'customer_id', 'product_id']:
                df[col] = df[col].str.strip().str.lower()
        
        return df
    
    def _parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse and validate date columns"""
        date_columns = ['order_date', 'ship_date', 'delivery_date', 'created_at', 'updated_at']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Fill missing dates with reasonable defaults
                if col == 'created_at' and df[col].isna().any():
                    df[col] = df[col].fillna(datetime.now())
        
        return df
    
    def _calculate_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived fields"""
        # Total amount calculation
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            df['line_total'] = df['quantity'] * df['unit_price']
        
        # Discount amount
        if 'discount_percent' in df.columns and 'line_total' in df.columns:
            df['discount_amount'] = df['line_total'] * (df['discount_percent'] / 100)
            df['final_amount'] = df['line_total'] - df['discount_amount']
        elif 'line_total' in df.columns:
            df['discount_amount'] = 0
            df['final_amount'] = df['line_total']
        
        # Tax calculation (assuming 8% tax rate)
        if 'final_amount' in df.columns:
            df['tax_amount'] = df['final_amount'] * 0.08
            df['total_amount'] = df['final_amount'] + df['tax_amount']
        
        # Shipping days calculation
        if 'order_date' in df.columns and 'ship_date' in df.columns:
            df['days_to_ship'] = (df['ship_date'] - df['order_date']).dt.days
        
        if 'ship_date' in df.columns and 'delivery_date' in df.columns:
            df['shipping_duration'] = (df['delivery_date'] - df['ship_date']).dt.days
        
        if 'order_date' in df.columns and 'delivery_date' in df.columns:
            df['total_fulfillment_days'] = (df['delivery_date'] - df['order_date']).dt.days
        
        return df
    
    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add time-based features for analysis"""
        if 'order_date' not in df.columns:
            return df
        
        df['order_year'] = df['order_date'].dt.year
        df['order_month'] = df['order_date'].dt.month
        df['order_quarter'] = df['order_date'].dt.quarter
        df['order_week'] = df['order_date'].dt.isocalendar().week
        df['order_day_of_week'] = df['order_date'].dt.dayofweek
        df['order_day_of_month'] = df['order_date'].dt.day
        df['order_hour'] = df['order_date'].dt.hour
        
        # Derived time features
        df['is_weekend'] = df['order_day_of_week'].isin([5, 6])
        df['is_month_start'] = df['order_day_of_month'] <= 7
        df['is_month_end'] = df['order_day_of_month'] >= 24
        
        # Season
        df['season'] = df['order_month'].map({
            12: 'winter', 1: 'winter', 2: 'winter',
            3: 'spring', 4: 'spring', 5: 'spring',
            6: 'summer', 7: 'summer', 8: 'summer',
            9: 'fall', 10: 'fall', 11: 'fall'
        })
        
        return df
    
    def _categorize_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize orders based on various criteria"""
        if 'total_amount' not in df.columns:
            return df
        
        # Order value tier
        df['order_value_tier'] = pd.cut(
            df['total_amount'],
            bins=[0, 50, 100, 250, 500, float('inf')],
            labels=['Very Low', 'Low', 'Medium', 'High', 'Very High']
        )
        
        # Order status flags
        df['is_completed'] = df['status'].isin(['completed', 'delivered', 'shipped']) if 'status' in df.columns else False
        df['is_cancelled'] = df['status'] == 'cancelled' if 'status' in df.columns else False
        df['is_pending'] = df['status'].isin(['pending', 'processing']) if 'status' in df.columns else False
        df['is_returned'] = df['status'] == 'returned' if 'status' in df.columns else False
        
        # Shipping speed category
        if 'days_to_ship' in df.columns:
            df['shipping_speed'] = pd.cut(
                df['days_to_ship'],
                bins=[-1, 1, 3, 7, float('inf')],
                labels=['Same Day', 'Express', 'Standard', 'Delayed']
            )
        
        # Quantity tier
        if 'quantity' in df.columns:
            df['quantity_tier'] = pd.cut(
                df['quantity'],
                bins=[0, 1, 3, 10, float('inf')],
                labels=['Single', 'Few', 'Multiple', 'Bulk']
            )
        
        return df
    
    def _convert_currency(self, df: pd.DataFrame, target_currency: str = 'USD') -> pd.DataFrame:
        """Convert all monetary values to target currency"""
        if 'currency' not in df.columns:
            df['currency'] = 'USD'
            df['original_currency'] = 'USD'
            return df
        
        df['original_currency'] = df['currency']
        df['exchange_rate'] = df['currency'].map(self.currency_rates).fillna(1.0)
        
        monetary_columns = ['line_total', 'discount_amount', 'final_amount', 
                          'tax_amount', 'total_amount', 'unit_price']
        
        for col in monetary_columns:
            if col in df.columns:
                df[f'{col}_usd'] = df[col] * df['exchange_rate']
        
        df['currency'] = target_currency
        
        return df
    
    def _generate_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate surrogate keys for dimensional modeling"""
        # Order surrogate key
        if 'order_id' in df.columns:
            df['order_sk'] = df['order_id'].apply(
                lambda x: int(hashlib.md5(str(x).encode()).hexdigest()[:8], 16)
            )
        
        # Date key for date dimension
        if 'order_date' in df.columns:
            df['date_key'] = df['order_date'].dt.strftime('%Y%m%d').astype(int)
        
        return df


class CustomerTransformer(DataTransformer):
    """Transformer for customer data"""
    
    @timing_decorator
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer data"""
        logger.info(f"Starting customer transformation with {len(df)} records")
        input_rows = len(df)
        
        result = df.copy()
        
        # Standardize columns
        result.columns = [col.lower().replace(' ', '_') for col in result.columns]
        
        # Clean email addresses
        if 'email' in result.columns:
            result['email'] = result['email'].str.lower().str.strip()
            result['email_domain'] = result['email'].str.split('@').str[-1]
            result['is_valid_email'] = result['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', regex=True, na=False)
        
        # Standardize phone numbers
        if 'phone' in result.columns:
            result['phone_clean'] = result['phone'].str.replace(r'[^\d]', '', regex=True)
            result['has_valid_phone'] = result['phone_clean'].str.len() >= 10
        
        # Parse names
        if 'full_name' in result.columns:
            name_parts = result['full_name'].str.split(' ', n=1, expand=True)
            result['first_name'] = name_parts[0] if 0 in name_parts.columns else ''
            result['last_name'] = name_parts[1] if 1 in name_parts.columns else ''
        
        # Standardize addresses
        result = self._standardize_address(result)
        
        # Calculate customer age
        if 'birth_date' in result.columns:
            result['birth_date'] = pd.to_datetime(result['birth_date'], errors='coerce')
            result['age'] = ((datetime.now() - result['birth_date']).dt.days / 365.25).astype(int)
            result['age_group'] = pd.cut(
                result['age'],
                bins=[0, 18, 25, 35, 45, 55, 65, 100],
                labels=['Under 18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
            )
        
        # Registration date features
        if 'registration_date' in result.columns:
            result['registration_date'] = pd.to_datetime(result['registration_date'], errors='coerce')
            result['customer_tenure_days'] = (datetime.now() - result['registration_date']).dt.days
            result['customer_tenure_months'] = (result['customer_tenure_days'] / 30).astype(int)
            result['is_new_customer'] = result['customer_tenure_days'] <= 30
        
        # Generate customer key
        if 'customer_id' in result.columns:
            result['customer_sk'] = result['customer_id'].apply(
                lambda x: int(hashlib.md5(str(x).encode()).hexdigest()[:8], 16)
            )
        
        self.log_transformation('transform_customers', input_rows, len(result))
        return result
    
    def _standardize_address(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize address fields"""
        if 'country' in df.columns:
            country_mapping = {
                'usa': 'United States',
                'us': 'United States',
                'united states of america': 'United States',
                'uk': 'United Kingdom',
                'britain': 'United Kingdom',
                'great britain': 'United Kingdom'
            }
            df['country_standardized'] = df['country'].str.lower().str.strip().map(country_mapping)
            df['country_standardized'] = df['country_standardized'].fillna(df['country'])
        
        if 'state' in df.columns:
            df['state'] = df['state'].str.upper().str.strip()
        
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str).str[:5]
        
        return df


class ProductTransformer(DataTransformer):
    """Transformer for product data"""
    
    @timing_decorator
    def transform_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform product data"""
        logger.info(f"Starting product transformation with {len(df)} records")
        input_rows = len(df)
        
        result = df.copy()
        
        # Standardize columns
        result.columns = [col.lower().replace(' ', '_') for col in result.columns]
        
        # Clean product names
        if 'product_name' in result.columns:
            result['product_name_clean'] = result['product_name'].str.strip()
            result['product_name_lower'] = result['product_name_clean'].str.lower()
        
        # Categorize products
        if 'category' in result.columns:
            result['category'] = result['category'].str.strip().str.title()
            result['main_category'] = result['category'].str.split('>').str[0].str.strip()
        
        # Price analysis
        if 'price' in result.columns:
            result['price'] = pd.to_numeric(result['price'], errors='coerce')
            result['price_tier'] = pd.cut(
                result['price'],
                bins=[0, 25, 50, 100, 250, 500, float('inf')],
                labels=['Budget', 'Economy', 'Standard', 'Premium', 'Luxury', 'Ultra Luxury']
            )
        
        # Calculate profit margin if cost is available
        if 'price' in result.columns and 'cost' in result.columns:
            result['profit_margin'] = ((result['price'] - result['cost']) / result['price'] * 100).round(2)
            result['margin_tier'] = pd.cut(
                result['profit_margin'],
                bins=[-float('inf'), 0, 20, 40, 60, 100],
                labels=['Loss', 'Low Margin', 'Medium Margin', 'High Margin', 'Very High Margin']
            )
        
        # Stock status
        if 'stock_quantity' in result.columns:
            result['is_in_stock'] = result['stock_quantity'] > 0
            result['is_low_stock'] = (result['stock_quantity'] > 0) & (result['stock_quantity'] <= 10)
            result['is_out_of_stock'] = result['stock_quantity'] == 0
            result['stock_status'] = pd.cut(
                result['stock_quantity'],
                bins=[-1, 0, 10, 50, 100, float('inf')],
                labels=['Out of Stock', 'Low Stock', 'Medium Stock', 'Good Stock', 'Overstocked']
            )
        
        # Generate product key
        if 'product_id' in result.columns:
            result['product_sk'] = result['product_id'].apply(
                lambda x: int(hashlib.md5(str(x).encode()).hexdigest()[:8], 16)
            )
        
        self.log_transformation('transform_products', input_rows, len(result))
        return result


class AggregationTransformer(DataTransformer):
    """Transformer for creating aggregated datasets"""
    
    @timing_decorator
    def create_customer_summary(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Create customer summary metrics"""
        logger.info("Creating customer summary")
        
        summary = orders_df.groupby('customer_id').agg({
            'order_id': 'count',
            'total_amount': ['sum', 'mean', 'min', 'max'],
            'quantity': 'sum',
            'order_date': ['min', 'max']
        }).reset_index()
        
        # Flatten column names
        summary.columns = [
            'customer_id', 'total_orders', 'total_revenue', 
            'avg_order_value', 'min_order_value', 'max_order_value',
            'total_items_purchased', 'first_order_date', 'last_order_date'
        ]
        
        # Calculate additional metrics
        summary['customer_lifetime_days'] = (
            summary['last_order_date'] - summary['first_order_date']
        ).dt.days
        
        summary['avg_days_between_orders'] = (
            summary['customer_lifetime_days'] / (summary['total_orders'] - 1)
        ).replace([np.inf, -np.inf], np.nan)
        
        # Customer segments
        summary['customer_segment'] = pd.cut(
            summary['total_revenue'],
            bins=[0, 100, 500, 2000, float('inf')],
            labels=['Bronze', 'Silver', 'Gold', 'Platinum']
        )
        
        # Recency calculation
        summary['days_since_last_order'] = (
            datetime.now() - summary['last_order_date']
        ).dt.days
        
        # RFM scoring
        summary['recency_score'] = pd.qcut(
            summary['days_since_last_order'].rank(method='first'),
            q=5, labels=[5, 4, 3, 2, 1]
        )
        summary['frequency_score'] = pd.qcut(
            summary['total_orders'].rank(method='first'),
            q=5, labels=[1, 2, 3, 4, 5]
        )
        summary['monetary_score'] = pd.qcut(
            summary['total_revenue'].rank(method='first'),
            q=5, labels=[1, 2, 3, 4, 5]
        )
        
        summary['rfm_score'] = (
            summary['recency_score'].astype(int) * 100 +
            summary['frequency_score'].astype(int) * 10 +
            summary['monetary_score'].astype(int)
        )
        
        return summary
    
    @timing_decorator
    def create_product_summary(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Create product summary metrics"""
        logger.info("Creating product summary")
        
        summary = orders_df.groupby('product_id').agg({
            'order_id': 'count',
            'quantity': 'sum',
            'total_amount': 'sum',
            'customer_id': 'nunique',
            'order_date': ['min', 'max']
        }).reset_index()
        
        summary.columns = [
            'product_id', 'total_orders', 'total_quantity_sold',
            'total_revenue', 'unique_customers', 'first_sale_date', 'last_sale_date'
        ]
        
        # Calculate additional metrics
        summary['avg_quantity_per_order'] = (
            summary['total_quantity_sold'] / summary['total_orders']
        ).round(2)
        
        summary['revenue_per_unit'] = (
            summary['total_revenue'] / summary['total_quantity_sold']
        ).round(2)
        
        summary['days_since_first_sale'] = (
            datetime.now() - summary['first_sale_date']
        ).dt.days
        
        summary['days_since_last_sale'] = (
            datetime.now() - summary['last_sale_date']
        ).dt.days
        
        # Product performance tier
        summary['performance_tier'] = pd.cut(
            summary['total_revenue'],
            bins=[0, 1000, 5000, 20000, float('inf')],
            labels=['Low Performer', 'Medium Performer', 'High Performer', 'Top Performer']
        )
        
        return summary
    
    @timing_decorator
    def create_daily_summary(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Create daily aggregated metrics"""
        logger.info("Creating daily summary")
        
        orders_df['order_date_only'] = orders_df['order_date'].dt.date
        
        summary = orders_df.groupby('order_date_only').agg({
            'order_id': 'count',
            'total_amount': 'sum',
            'quantity': 'sum',
            'customer_id': 'nunique',
            'product_id': 'nunique',
            'discount_amount': 'sum'
        }).reset_index()
        
        summary.columns = [
            'date', 'total_orders', 'total_revenue', 'total_items_sold',
            'unique_customers', 'unique_products', 'total_discounts'
        ]
        
        summary['avg_order_value'] = (summary['total_revenue'] / summary['total_orders']).round(2)
        summary['avg_items_per_order'] = (summary['total_items_sold'] / summary['total_orders']).round(2)
        
        # Day over day changes
        summary['revenue_change'] = summary['total_revenue'].pct_change() * 100
        summary['orders_change'] = summary['total_orders'].pct_change() * 100
        
        # Rolling averages
        summary['revenue_7d_avg'] = summary['total_revenue'].rolling(window=7).mean()
        summary['orders_7d_avg'] = summary['total_orders'].rolling(window=7).mean()
        
        return summary


class DataEnricher:
    """Class for enriching data with additional information"""
    
    @staticmethod
    def enrich_with_geolocation(df: pd.DataFrame, geo_data: pd.DataFrame) -> pd.DataFrame:
        """Enrich data with geolocation information"""
        if 'zip_code' in df.columns and 'zip_code' in geo_data.columns:
            return df.merge(
                geo_data[['zip_code', 'latitude', 'longitude', 'city', 'state', 'timezone']],
                on='zip_code',
                how='left'
            )
        return df
    
    @staticmethod
    def enrich_with_weather(df: pd.DataFrame, weather_data: pd.DataFrame) -> pd.DataFrame:
        """Enrich data with weather information"""
        if 'order_date' in df.columns and 'date' in weather_data.columns:
            df['order_date_only'] = df['order_date'].dt.date
            weather_data['date'] = pd.to_datetime(weather_data['date']).dt.date
            
            return df.merge(
                weather_data,
                left_on='order_date_only',
                right_on='date',
                how='left'
            )
        return df
    
    @staticmethod
    def enrich_with_holidays(df: pd.DataFrame, holidays: List[str]) -> pd.DataFrame:
        """Mark orders that occurred on holidays"""
        if 'order_date' in df.columns:
            holiday_dates = pd.to_datetime(holidays).date
            df['is_holiday'] = df['order_date'].dt.date.isin(holiday_dates)
        return df


# Utility functions for common transformations

def clean_text_column(series: pd.Series) -> pd.Series:
    """Clean text column by removing special characters and extra whitespace"""
    return series.str.strip().str.replace(r'\s+', ' ', regex=True).str.replace(r'[^\w\s]', '', regex=True)


def normalize_numeric_column(series: pd.Series) -> pd.Series:
    """Normalize numeric column to 0-1 range"""
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(series))
    return (series - min_val) / (max_val - min_val)


def standardize_numeric_column(series: pd.Series) -> pd.Series:
    """Standardize numeric column (z-score normalization)"""
    mean_val = series.mean()
    std_val = series.std()
    if std_val == 0:
        return pd.Series([0] * len(series))
    return (series - mean_val) / std_val


def create_date_dimension(start_date: str, end_date: str) -> pd.DataFrame:
    """Create a date dimension table"""
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    date_dim = pd.DataFrame({'date': dates})
    date_dim['date_key'] = date_dim['date'].dt.strftime('%Y%m%d').astype(int)
    date_dim['year'] = date_dim['date'].dt.year
    date_dim['quarter'] = date_dim['date'].dt.quarter
    date_dim['month'] = date_dim['date'].dt.month
    date_dim['month_name'] = date_dim['date'].dt.month_name()
    date_dim['week'] = date_dim['date'].dt.isocalendar().week
    date_dim['day_of_year'] = date_dim['date'].dt.dayofyear
    date_dim['day_of_month'] = date_dim['date'].dt.day
    date_dim['day_of_week'] = date_dim['date'].dt.dayofweek
    date_dim['day_name'] = date_dim['date'].dt.day_name()
    date_dim['is_weekend'] = date_dim['day_of_week'].isin([5, 6])
    date_dim['is_month_start'] = date_dim['date'].dt.is_month_start
    date_dim['is_month_end'] = date_dim['date'].dt.is_month_end
    date_dim['is_quarter_start'] = date_dim['date'].dt.is_quarter_start
    date_dim['is_quarter_end'] = date_dim['date'].dt.is_quarter_end
    date_dim['is_year_start'] = date_dim['date'].dt.is_year_start
    date_dim['is_year_end'] = date_dim['date'].dt.is_year_end
    
    # Fiscal year (assuming fiscal year starts in April)
    date_dim['fiscal_year'] = date_dim.apply(
        lambda x: x['year'] if x['month'] >= 4 else x['year'] - 1, axis=1
    )
    date_dim['fiscal_quarter'] = date_dim['month'].apply(
        lambda x: ((x - 4) % 12) // 3 + 1
    )
    
    return date_dim


if __name__ == "__main__":
    # Example usage
    import json
    
    # Sample order data
    sample_orders = pd.DataFrame({
        'order_id': ['ORD001', 'ORD002', 'ORD003'],
        'customer_id': ['CUST001', 'CUST002', 'CUST001'],
        'product_id': ['PROD001', 'PROD002', 'PROD003'],
        'quantity': [2, 1, 5],
        'unit_price': [29.99, 149.99, 9.99],
        'discount_percent': [0, 10, 5],
        'order_date': ['2024-01-15 10:30:00', '2024-01-16 14:45:00', '2024-01-17 09:00:00'],
        'status': ['completed', 'shipped', 'pending'],
        'currency': ['USD', 'EUR', 'USD']
    })
    
    # Transform orders
    transformer = OrderTransformer()
    transformed_orders = transformer.transform_orders(sample_orders)
    
    print("\nTransformed Orders:")
    print(transformed_orders.head())
    print("\nNew Columns:")
    print([col for col in transformed_orders.columns if col not in sample_orders.columns])
"""
API data ingestion module.
Handles fetching data from REST APIs with retry logic and pagination.
"""

import asyncio
import aiohttp
import requests
from typing import List, Dict, Any, Optional, Generator
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
import json

from src.config import settings
from src.utils.logging_utils import logger, log_execution_time, log_execution_time_async


class APIIngestionClient:
    """Client for ingesting data from REST APIs."""
    
    def __init__(
        self,
        base_url: str = None,
        timeout: int = 30,
        retry_attempts: int = 3,
        headers: Optional[Dict[str, str]] = None
    ):
        self.base_url = base_url or settings.api.base_url
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.headers = headers or {'Content-Type': 'application/json'}
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                headers=self.headers,
                timeout=timeout
            )
        return self._session
    
    async def close(self):
        """Close the session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    @log_execution_time
    def fetch_data_sync(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Synchronously fetch data from API endpoint.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        logger.info(f"Fetching data from {url}")
        
        response = requests.get(
            url,
            params=params,
            headers=self.headers,
            timeout=self.timeout
        )
        response.raise_for_status()
        
        return response.json()
    
    @log_execution_time_async
    async def fetch_data_async(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Asynchronously fetch data from API endpoint.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            API response as dictionary
        """
        session = await self._get_session()
        url = f"{self.base_url}{endpoint}"
        
        logger.info(f"Async fetching data from {url}")
        
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
    
    @log_execution_time
    def fetch_paginated_data(
        self,
        endpoint: str,
        page_param: str = 'page',
        limit_param: str = 'limit',
        limit: int = 100,
        max_pages: Optional[int] = None,
        data_key: Optional[str] = None
    ) -> Generator[List[Dict], None, None]:
        """
        Fetch paginated data from API.
        
        Args:
            endpoint: API endpoint
            page_param: Parameter name for page number
            limit_param: Parameter name for page size
            limit: Number of records per page
            max_pages: Maximum number of pages to fetch
            data_key: Key in response containing data list
            
        Yields:
            List of records per page
        """
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
            
            params = {page_param: page, limit_param: limit}
            
            try:
                response = self.fetch_data_sync(endpoint, params)
                
                # Extract data from response
                data = response.get(data_key, response) if data_key else response
                
                if not data or (isinstance(data, list) and len(data) == 0):
                    break
                
                yield data
                
                # Check if more pages exist
                if isinstance(data, list) and len(data) < limit:
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
    
    async def fetch_multiple_endpoints_async(
        self,
        endpoints: List[str]
    ) -> Dict[str, Any]:
        """
        Fetch data from multiple endpoints concurrently.
        
        Args:
            endpoints: List of endpoints to fetch
            
        Returns:
            Dictionary mapping endpoints to their responses
        """
        tasks = [self.fetch_data_async(endpoint) for endpoint in endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            endpoint: result 
            for endpoint, result in zip(endpoints, results)
            if not isinstance(result, Exception)
        }


class OrdersAPIIngestion(APIIngestionClient):
    """Specialized client for ingesting orders data."""
    
    def __init__(self):
        super().__init__()
        self.endpoint = '/api/orders'
    
    @log_execution_time
    def fetch_orders(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch orders within date range.
        
        Args:
            start_date: Start date for filtering
            end_date: End date for filtering
            status: Order status filter
            
        Returns:
            List of order records
        """
        params = {}
        
        if start_date:
            params['start_date'] = start_date.isoformat()
        if end_date:
            params['end_date'] = end_date.isoformat()
        if status:
            params['status'] = status
        
        all_orders = []
        
        for page_data in self.fetch_paginated_data(
            self.endpoint,
            data_key='orders'
        ):
            all_orders.extend(page_data)
        
        logger.info(f"Fetched {len(all_orders)} orders")
        return all_orders
    
    @log_execution_time
    def fetch_recent_orders(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Fetch orders from the last N hours.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of recent order records
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=hours)
        
        return self.fetch_orders(start_date=start_date, end_date=end_date)
    
    def fetch_order_details(self, order_id: str) -> Dict[str, Any]:
        """
        Fetch details for a specific order.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order details dictionary
        """
        return self.fetch_data_sync(f"{self.endpoint}/{order_id}")


class ProductsAPIIngestion(APIIngestionClient):
    """Specialized client for ingesting products data."""
    
    def __init__(self):
        super().__init__()
        self.endpoint = '/api/products'
    
    @log_execution_time
    def fetch_all_products(self) -> List[Dict[str, Any]]:
        """
        Fetch all products.
        
        Returns:
            List of product records
        """
        all_products = []
        
        for page_data in self.fetch_paginated_data(
            self.endpoint,
            data_key='products'
        ):
            all_products.extend(page_data)
        
        logger.info(f"Fetched {len(all_products)} products")
        return all_products
    
    @log_execution_time
    def fetch_products_by_category(
        self,
        category: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch products by category.
        
        Args:
            category: Product category
            
        Returns:
            List of product records
        """
        params = {'category': category}
        response = self.fetch_data_sync(self.endpoint, params)
        return response.get('products', [])


class EventsAPIIngestion(APIIngestionClient):
    """Specialized client for ingesting user events data."""
    
    def __init__(self):
        super().__init__()
        self.endpoint = '/api/events'
    
    @log_execution_time
    def fetch_events(
        self,
        event_type: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch user events.
        
        Args:
            event_type: Type of event to filter
            start_time: Start time for filtering
            end_time: End time for filtering
            
        Returns:
            List of event records
        """
        params = {}
        
        if event_type:
            params['event_type'] = event_type
        if start_time:
            params['start_time'] = start_time.isoformat()
        if end_time:
            params['end_time'] = end_time.isoformat()
        
        all_events = []
        
        for page_data in self.fetch_paginated_data(
            self.endpoint,
            data_key='events'
        ):
            all_events.extend(page_data)
        
        logger.info(f"Fetched {len(all_events)} events")
        return all_events


# Convenience instances
orders_ingestion = OrdersAPIIngestion()
products_ingestion = ProductsAPIIngestion()
events_ingestion = EventsAPIIngestion()


__all__ = [
    'APIIngestionClient',
    'OrdersAPIIngestion',
    'ProductsAPIIngestion',
    'EventsAPIIngestion',
    'orders_ingestion',
    'products_ingestion',
    'events_ingestion'
]
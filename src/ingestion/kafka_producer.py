"""
Kafka producer module for streaming data ingestion.
Publishes data to Kafka topics for real-time processing.
"""

import json
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError
from confluent_kafka import Producer as ConfluentProducer
import threading
import time

from src.config import settings
from src.utils.logging_utils import logger, log_execution_time


class DataKafkaProducer:
    """Kafka producer for publishing data to topics."""
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        client_id: str = 'ecommerce-producer'
    ):
        self.bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self.client_id = client_id
        self._producer: Optional[KafkaProducer] = None
        self._confluent_producer: Optional[ConfluentProducer] = None
        self._delivery_reports: Dict[str, bool] = {}
        self._lock = threading.Lock()
    
    @property
    def producer(self) -> KafkaProducer:
        """Get or create Kafka producer."""
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip',
                batch_size=16384,
                linger_ms=10
            )
        return self._producer
    
    def _create_message(
        self,
        data: Dict[str, Any],
        message_type: str,
        source: str = 'ecommerce-pipeline'
    ) -> Dict[str, Any]:
        """
        Create a standardized message envelope.
        
        Args:
            data: Message payload
            message_type: Type of message
            source: Source system identifier
            
        Returns:
            Message envelope with metadata
        """
        return {
            'message_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'source': source,
            'type': message_type,
            'data': data
        }
    
    @log_execution_time
    def send(
        self,
        topic: str,
        data: Dict[str, Any],
        key: Optional[str] = None,
        message_type: str = 'event',
        wait_for_ack: bool = False
    ) -> bool:
        """
        Send a message to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            data: Message payload
            key: Message key for partitioning
            message_type: Type of message
            wait_for_ack: Wait for acknowledgment
            
        Returns:
            True if successful, False otherwise
        """
        try:
            message = self._create_message(data, message_type)
            
            future = self.producer.send(topic, value=message, key=key)
            
            if wait_for_ack:
                record_metadata = future.get(timeout=10)
                logger.debug(
                    f"Message sent to {record_metadata.topic}[{record_metadata.partition}] "
                    f"offset {record_metadata.offset}"
                )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return False
    
    @log_execution_time
    def send_batch(
        self,
        topic: str,
        records: List[Dict[str, Any]],
        key_field: Optional[str] = None,
        message_type: str = 'event'
    ) -> int:
        """
        Send multiple messages in batch.
        
        Args:
            topic: Kafka topic name
            records: List of message payloads
            key_field: Field in data to use as message key
            message_type: Type of messages
            
        Returns:
            Number of successfully sent messages
        """
        successful = 0
        
        for record in records:
            key = str(record.get(key_field)) if key_field else None
            
            if self.send(topic, record, key=key, message_type=message_type):
                successful += 1
        
        # Ensure all messages are sent
        self.producer.flush()
        
        logger.info(f"Sent {successful}/{len(records)} messages to {topic}")
        return successful
    
    def flush(self, timeout: float = 30.0):
        """Flush pending messages."""
        if self._producer:
            self._producer.flush(timeout=timeout)
    
    def close(self):
        """Close the producer."""
        if self._producer:
            self._producer.close()
            self._producer = None


class OrdersProducer(DataKafkaProducer):
    """Specialized producer for order events."""
    
    def __init__(self):
        super().__init__(client_id='orders-producer')
        self.topic = settings.kafka.topics['orders']
    
    @log_execution_time
    def publish_order_created(self, order: Dict[str, Any]) -> bool:
        """
        Publish order created event.
        
        Args:
            order: Order data
            
        Returns:
            True if successful
        """
        return self.send(
            topic=self.topic,
            data=order,
            key=str(order.get('order_id')),
            message_type='order_created',
            wait_for_ack=True
        )
    
    @log_execution_time
    def publish_order_updated(
        self,
        order_id: str,
        updates: Dict[str, Any]
    ) -> bool:
        """
        Publish order updated event.
        
        Args:
            order_id: Order ID
            updates: Updated fields
            
        Returns:
            True if successful
        """
        data = {
            'order_id': order_id,
            'updates': updates
        }
        
        return self.send(
            topic=self.topic,
            data=data,
            key=order_id,
            message_type='order_updated',
            wait_for_ack=True
        )
    
    @log_execution_time
    def publish_orders_batch(
        self,
        orders: List[Dict[str, Any]]
    ) -> int:
        """
        Publish multiple orders.
        
        Args:
            orders: List of order data
            
        Returns:
            Number of successfully published orders
        """
        return self.send_batch(
            topic=self.topic,
            records=orders,
            key_field='order_id',
            message_type='order_created'
        )


class EventsProducer(DataKafkaProducer):
    """Specialized producer for user events."""
    
    def __init__(self):
        super().__init__(client_id='events-producer')
        self.topic = settings.kafka.topics['events']
    
    @log_execution_time
    def publish_page_view(
        self,
        user_id: str,
        page: str,
        session_id: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        """
        Publish page view event.
        
        Args:
            user_id: User ID
            page: Page URL
            session_id: Session ID
            metadata: Additional metadata
            
        Returns:
            True if successful
        """
        data = {
            'user_id': user_id,
            'page': page,
            'session_id': session_id,
            'event_type': 'page_view',
            'metadata': metadata or {}
        }
        
        return self.send(
            topic=self.topic,
            data=data,
            key=user_id,
            message_type='page_view'
        )
    
    @log_execution_time
    def publish_product_view(
        self,
        user_id: str,
        product_id: str,
        session_id: str
    ) -> bool:
        """
        Publish product view event.
        
        Args:
            user_id: User ID
            product_id: Product ID
            session_id: Session ID
            
        Returns:
            True if successful
        """
        data = {
            'user_id': user_id,
            'product_id': product_id,
            'session_id': session_id,
            'event_type': 'product_view'
        }
        
        return self.send(
            topic=self.topic,
            data=data,
            key=user_id,
            message_type='product_view'
        )
    
    @log_execution_time
    def publish_add_to_cart(
        self,
        user_id: str,
        product_id: str,
        quantity: int,
        session_id: str
    ) -> bool:
        """
        Publish add to cart event.
        
        Args:
            user_id: User ID
            product_id: Product ID
            quantity: Quantity added
            session_id: Session ID
            
        Returns:
            True if successful
        """
        data = {
            'user_id': user_id,
            'product_id': product_id,
            'quantity': quantity,
            'session_id': session_id,
            'event_type': 'add_to_cart'
        }
        
        return self.send(
            topic=self.topic,
            data=data,
            key=user_id,
            message_type='add_to_cart'
        )
    
    @log_execution_time
    def publish_checkout_event(
        self,
        user_id: str,
        order_id: str,
        event_type: str,
        session_id: str
    ) -> bool:
        """
        Publish checkout event.
        
        Args:
            user_id: User ID
            order_id: Order ID
            event_type: Type of checkout event
            session_id: Session ID
            
        Returns:
            True if successful
        """
        data = {
            'user_id': user_id,
            'order_id': order_id,
            'event_type': event_type,
            'session_id': session_id
        }
        
        return self.send(
            topic=self.topic,
            data=data,
            key=user_id,
            message_type=event_type
        )


class ProductsProducer(DataKafkaProducer):
    """Specialized producer for product events."""
    
    def __init__(self):
        super().__init__(client_id='products-producer')
        self.topic = settings.kafka.topics['products']
    
    @log_execution_time
    def publish_product_update(
        self,
        product: Dict[str, Any]
    ) -> bool:
        """
        Publish product update event.
        
        Args:
            product: Product data
            
        Returns:
            True if successful
        """
        return self.send(
            topic=self.topic,
            data=product,
            key=str(product.get('product_id')),
            message_type='product_update',
            wait_for_ack=True
        )
    
    @log_execution_time
    def publish_inventory_update(
        self,
        product_id: str,
        stock_quantity: int,
        reserved_quantity: int = 0
    ) -> bool:
        """
        Publish inventory update event.
        
        Args:
            product_id: Product ID
            stock_quantity: Current stock quantity
            reserved_quantity: Reserved quantity
            
        Returns:
            True if successful
        """
        data = {
            'product_id': product_id,
            'stock_quantity': stock_quantity,
            'reserved_quantity': reserved_quantity,
            'available_quantity': stock_quantity - reserved_quantity
        }
        
        return self.send(
            topic=self.topic,
            data=data,
            key=product_id,
            message_type='inventory_update',
            wait_for_ack=True
        )


# Convenience instances
orders_producer = OrdersProducer()
events_producer = EventsProducer()
products_producer = ProductsProducer()


__all__ = [
    'DataKafkaProducer',
    'OrdersProducer',
    'EventsProducer',
    'ProductsProducer',
    'orders_producer',
    'events_producer',
    'products_producer'
]
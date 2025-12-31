#!/usr/bin/env python3
"""
Kafka Event Producer - Simulates real-time e-commerce events
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Sample data
CUSTOMER_IDS = [f'CUST{i:03d}' for i in range(1, 11)]
PRODUCT_IDS = [f'PROD{i:03d}' for i in range(1, 11)]
EVENT_TYPES = ['page_view', 'add_to_cart', 'remove_from_cart', 'purchase', 'search']
PAGES = ['home', 'product_list', 'product_detail', 'cart', 'checkout', 'confirmation']


def create_producer():
    """Create Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )


def generate_event():
    """Generate a random e-commerce event."""
    event_type = random.choice(EVENT_TYPES)
    
    event = {
        'event_id': f"EVT-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
        'event_type': event_type,
        'timestamp': datetime.now().isoformat(),
        'customer_id': random.choice(CUSTOMER_IDS),
        'session_id': f"SES-{random.randint(10000, 99999)}",
        'device': random.choice(['desktop', 'mobile', 'tablet']),
        'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
    }
    
    if event_type == 'page_view':
        event['page'] = random.choice(PAGES)
        event['referrer'] = random.choice(['google', 'facebook', 'direct', 'email'])
        
    elif event_type in ['add_to_cart', 'remove_from_cart']:
        event['product_id'] = random.choice(PRODUCT_IDS)
        event['quantity'] = random.randint(1, 5)
        event['price'] = round(random.uniform(10, 200), 2)
        
    elif event_type == 'purchase':
        event['order_id'] = f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        event['total_amount'] = round(random.uniform(20, 500), 2)
        event['payment_method'] = random.choice(['credit_card', 'paypal', 'debit_card'])
        
    elif event_type == 'search':
        event['search_query'] = random.choice(['headphones', 'running shoes', 'laptop', 'tea', 'watch'])
        event['results_count'] = random.randint(0, 100)
    
    return event


def produce_events(num_events=100, delay=0.5):
    """Produce events to Kafka."""
    producer = create_producer()
    
    print(f"\n🚀 Starting Kafka Event Producer")
    print(f"   Sending {num_events} events to topic 'events'")
    print(f"   Delay between events: {delay}s")
    print("-" * 50)
    
    for i in range(num_events):
        event = generate_event()
        
        # Send to Kafka
        producer.send(
            topic='events',
            key=event['customer_id'],
            value=event
        )
        
        print(f"[{i+1}/{num_events}] Sent {event['event_type']:15} | Customer: {event['customer_id']} | {event['event_id']}")
        
        time.sleep(delay)
    
    producer.flush()
    producer.close()
    
    print("-" * 50)
    print(f"✅ Sent {num_events} events successfully!")


if __name__ == '__main__':
    # Send 20 events with 0.5 second delay
    produce_events(num_events=20, delay=0.5)#!/usr/bin/env python3
"""
Kafka Event Producer - Simulates real-time e-commerce events
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Sample data
CUSTOMER_IDS = [f'CUST{i:03d}' for i in range(1, 11)]
PRODUCT_IDS = [f'PROD{i:03d}' for i in range(1, 11)]
EVENT_TYPES = ['page_view', 'add_to_cart', 'remove_from_cart', 'purchase', 'search']
PAGES = ['home', 'product_list', 'product_detail', 'cart', 'checkout', 'confirmation']


def create_producer():
    """Create Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )


def generate_event():
    """Generate a random e-commerce event."""
    event_type = random.choice(EVENT_TYPES)
    
    event = {
        'event_id': f"EVT-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
        'event_type': event_type,
        'timestamp': datetime.now().isoformat(),
        'customer_id': random.choice(CUSTOMER_IDS),
        'session_id': f"SES-{random.randint(10000, 99999)}",
        'device': random.choice(['desktop', 'mobile', 'tablet']),
        'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
    }
    
    if event_type == 'page_view':
        event['page'] = random.choice(PAGES)
        event['referrer'] = random.choice(['google', 'facebook', 'direct', 'email'])
        
    elif event_type in ['add_to_cart', 'remove_from_cart']:
        event['product_id'] = random.choice(PRODUCT_IDS)
        event['quantity'] = random.randint(1, 5)
        event['price'] = round(random.uniform(10, 200), 2)
        
    elif event_type == 'purchase':
        event['order_id'] = f"ORD-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        event['total_amount'] = round(random.uniform(20, 500), 2)
        event['payment_method'] = random.choice(['credit_card', 'paypal', 'debit_card'])
        
    elif event_type == 'search':
        event['search_query'] = random.choice(['headphones', 'running shoes', 'laptop', 'tea', 'watch'])
        event['results_count'] = random.randint(0, 100)
    
    return event


def produce_events(num_events=100, delay=0.5):
    """Produce events to Kafka."""
    producer = create_producer()
    
    print(f"\n🚀 Starting Kafka Event Producer")
    print(f"   Sending {num_events} events to topic 'events'")
    print(f"   Delay between events: {delay}s")
    print("-" * 50)
    
    for i in range(num_events):
        event = generate_event()
        
        # Send to Kafka
        producer.send(
            topic='events',
            key=event['customer_id'],
            value=event
        )
        
        print(f"[{i+1}/{num_events}] Sent {event['event_type']:15} | Customer: {event['customer_id']} | {event['event_id']}")
        
        time.sleep(delay)
    
    producer.flush()
    producer.close()
    
    print("-" * 50)
    print(f"✅ Sent {num_events} events successfully!")


if __name__ == '__main__':
    # Send 20 events with 0.5 second delay
    produce_events(num_events=20, delay=0.5)
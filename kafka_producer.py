#!/usr/bin/env python3
"""
Simple Kafka Producer for Windows
Uses port 29092 (external listener)
"""

import json
import time
import random
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration - Use port 29092 for Windows access
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

# Sample data
CUSTOMER_IDS = [f'CUST{i:03d}' for i in range(1, 6)]
PRODUCT_IDS = [f'PROD{i:03d}' for i in range(1, 6)]
EVENT_TYPES = ['page_view', 'add_to_cart', 'purchase', 'search']


def generate_event():
    """Generate a random e-commerce event."""
    event_type = random.choice(EVENT_TYPES)
    
    event = {
        'event_id': f"EVT-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
        'event_type': event_type,
        'timestamp': datetime.now().isoformat(),
        'customer_id': random.choice(CUSTOMER_IDS),
        'session_id': f"SES-{random.randint(10000, 99999)}",
    }
    
    if event_type == 'page_view':
        event['page'] = random.choice(['home', 'product', 'cart', 'checkout'])
    elif event_type == 'add_to_cart':
        event['product_id'] = random.choice(PRODUCT_IDS)
        event['quantity'] = random.randint(1, 3)
    elif event_type == 'purchase':
        event['order_id'] = f"ORD-{random.randint(1000, 9999)}"
        event['total_amount'] = round(random.uniform(20, 300), 2)
    elif event_type == 'search':
        event['query'] = random.choice(['headphones', 'shoes', 'watch'])
    
    return event


def main():
    print("\n" + "=" * 60)
    print("🚀 Kafka Event Producer")
    print("=" * 60)
    print(f"\n📡 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    
    try:
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            request_timeout_ms=10000,
            retries=3,
        )
        print("   ✅ Connected successfully!\n")
        
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        print("\n📋 Troubleshooting:")
        print("   1. Check Kafka is running: docker ps | findstr kafka")
        print("   2. Check port 29092 is open: Test-NetConnection localhost -Port 29092")
        print("   3. Restart Kafka: docker-compose restart kafka")
        return
    
    # Send events
    topic = 'events'
    num_events = 20
    
    print(f"📨 Sending {num_events} events to topic '{topic}'...")
    print("-" * 60)
    
    for i in range(num_events):
        event = generate_event()
        
        try:
            future = producer.send(
                topic=topic,
                key=event['customer_id'],
                value=event
            )
            record = future.get(timeout=10)
            
            print(f"[{i+1:2}/{num_events}] ✅ {event['event_type']:12} | {event['customer_id']} | Partition {record.partition}")
            
        except Exception as e:
            print(f"[{i+1:2}/{num_events}] ❌ Failed: {e}")
        
        time.sleep(0.5)
    
    # Cleanup
    producer.flush()
    producer.close()
    
    print("-" * 60)
    print(f"\n✅ Done! Sent {num_events} events to '{topic}'")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    main()
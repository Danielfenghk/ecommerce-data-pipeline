"""
Hourly Streaming Pipeline DAG
Processes streaming data and maintains real-time aggregates
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def check_kafka_topics(**context):
    """Check if Kafka topics have data"""
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    
    topics = ['orders', 'events', 'inventory']
    topic_status = {}
    
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            consumer_timeout_ms=5000
        )
        
        available_topics = consumer.topics()
        
        for topic in topics:
            topic_status[topic] = topic in available_topics
        
        consumer.close()
        
    except KafkaError as e:
        logger.error(f"Kafka connection error: {e}")
        raise
    
    context['ti'].xcom_push(key='topic_status', value=topic_status)
    return topic_status


def consume_order_events(**context):
    """Consume and process order events from Kafka"""
    from kafka import KafkaConsumer
    import json
    
    execution_date = context['execution_date']
    hour_start = execution_date.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    
    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-hourly-pipeline',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000
    )
    
    events = []
    for message in consumer:
        event = message.value
        event_time = datetime.fromisoformat(event.get('timestamp', ''))
        
        if hour_start <= event_time < hour_end:
            events.append(event)
    
    consumer.close()
    
    context['ti'].xcom_push(key='order_events', value=events)
    logger.info(f"Consumed {len(events)} order events for {hour_start}")
    return len(events)


def process_streaming_batch(**context):
    """Process the batch of streaming events"""
    from src.processing.transformations import OrderTransformer
    import pandas as pd
    
    ti = context['ti']
    events = ti.xcom_pull(key='order_events', task_ids='consume_orders')
    
    if not events:
        logger.info("No events to process")
        return 0
    
    df = pd.DataFrame(events)
    
    transformer = OrderTransformer()
    transformed = transformer.transform(df)
    
    records = transformed.to_dict('records')
    ti.xcom_push(key='processed_events', value=records)
    
    return len(records)


def update_real_time_aggregates(**context):
    """Update real-time aggregate tables"""
    from src.warehouse.data_warehouse import DataWarehouse
    import pandas as pd
    
    ti = context['ti']
    events = ti.xcom_pull(key='processed_events', task_ids='process_batch')
    
    if not events:
        return
    
    df = pd.DataFrame(events)
    warehouse = DataWarehouse()
    
    # Calculate hourly aggregates
    hourly_agg = df.groupby(df['order_date'].dt.hour).agg({
        'total_amount': ['sum', 'mean', 'count'],
        'quantity': 'sum'
    }).reset_index()
    
    hourly_agg.columns = ['hour', 'total_revenue', 'avg_order_value', 
                          'order_count', 'total_items']
    hourly_agg['updated_at'] = datetime.now()
    
    warehouse.upsert_fact_table('agg_hourly_sales', hourly_agg, ['hour'])
    
    logger.info(f"Updated hourly aggregates with {len(hourly_agg)} records")


def sync_to_data_lake(**context):
    """Sync processed data to data lake"""
    from src.warehouse.data_warehouse import DataWarehouse
    import pandas as pd
    
    ti = context['ti']
    events = ti.xcom_pull(key='processed_events', task_ids='process_batch')
    
    if not events:
        return
    
    df = pd.DataFrame(events)
    warehouse = DataWarehouse()
    
    execution_date = context['execution_date']
    partition_path = f"year={execution_date.year}/month={execution_date.month:02d}/day={execution_date.day:02d}/hour={execution_date.hour:02d}"
    
    warehouse.write_to_lake(
        df,
        f"orders/processed/{partition_path}",
        format='parquet'
    )
    
    logger.info(f"Synced {len(df)} records to data lake")


# Create DAG
with DAG(
    'hourly_streaming_pipeline',
    default_args=default_args,
    description='Hourly processing of streaming data',
    schedule_interval='0 * * * *',  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['streaming', 'hourly', 'real-time'],
    max_active_runs=2,
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Check Kafka
    check_kafka = PythonOperator(
        task_id='check_kafka',
        python_callable=check_kafka_topics,
    )
    
    # Consume events
    consume_orders = PythonOperator(
        task_id='consume_orders',
        python_callable=consume_order_events,
    )
    
    # Process batch
    process_batch = PythonOperator(
        task_id='process_batch',
        python_callable=process_streaming_batch,
    )
    
    # Spark job for complex processing
    spark_processing = SparkSubmitOperator(
        task_id='spark_processing',
        application='/opt/airflow/spark_jobs/streaming_events_processing.py',
        conn_id='spark_default',
        conf={
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
        },
        application_args=[
            '--execution_date', '{{ ds }}',
            '--hour', '{{ execution_date.hour }}'
        ],
    )
    
    # Update aggregates
    update_aggregates = PythonOperator(
        task_id='update_aggregates',
        python_callable=update_real_time_aggregates,
    )
    
    # Sync to lake
    sync_lake = PythonOperator(
        task_id='sync_to_lake',
        python_callable=sync_to_data_lake,
    )
    
    end = DummyOperator(task_id='end')
    
    # Dependencies
    start >> check_kafka >> consume_orders >> process_batch
    process_batch >> [spark_processing, update_aggregates]
    [spark_processing, update_aggregates] >> sync_lake >> end
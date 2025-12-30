"""
Daily ETL Pipeline DAG
Orchestrates the daily data processing workflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}


def extract_orders(**context):
    """Extract orders from source database"""
    from src.ingestion.database_ingestion import DatabaseIngestion
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    logger.info(f"Extracting orders for date: {date_str}")
    
    ingestion = DatabaseIngestion()
    
    query = f"""
        SELECT 
            order_id, customer_id, product_id, 
            order_date, quantity, unit_price, 
            total_amount, status, shipping_address,
            created_at, updated_at
        FROM orders 
        WHERE DATE(order_date) = '{date_str}'
    """
    
    df = ingestion.ingest_from_query('source_db', query)
    
    # Store in XCom for downstream tasks
    records = df.to_dict('records')
    context['ti'].xcom_push(key='orders_data', value=records)
    context['ti'].xcom_push(key='orders_count', value=len(records))
    
    logger.info(f"Extracted {len(records)} orders")
    return len(records)


def extract_customers(**context):
    """Extract new/updated customers"""
    from src.ingestion.database_ingestion import DatabaseIngestion
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    ingestion = DatabaseIngestion()
    
    query = f"""
        SELECT 
            customer_id, email, first_name, last_name,
            phone, registration_date, segment,
            city, state, country, created_at, updated_at
        FROM customers 
        WHERE DATE(updated_at) >= '{date_str}'
           OR DATE(created_at) >= '{date_str}'
    """
    
    df = ingestion.ingest_from_query('source_db', query)
    records = df.to_dict('records')
    
    context['ti'].xcom_push(key='customers_data', value=records)
    context['ti'].xcom_push(key='customers_count', value=len(records))
    
    logger.info(f"Extracted {len(records)} customers")
    return len(records)


def extract_products(**context):
    """Extract product catalog"""
    from src.ingestion.file_ingestion import FileIngestion
    
    file_ingestion = FileIngestion()
    
    # Products come from CSV file
    df = file_ingestion.ingest_csv(
        '/data/products/products_catalog.csv',
        delimiter=',',
        encoding='utf-8'
    )
    
    records = df.to_dict('records')
    context['ti'].xcom_push(key='products_data', value=records)
    context['ti'].xcom_push(key='products_count', value=len(records))
    
    logger.info(f"Extracted {len(records)} products")
    return len(records)


def transform_orders(**context):
    """Transform orders data"""
    from src.processing.transformations import OrderTransformer
    
    ti = context['ti']
    orders_data = ti.xcom_pull(key='orders_data', task_ids='extract.extract_orders')
    
    if not orders_data:
        logger.warning("No orders data to transform")
        return 0
    
    df = pd.DataFrame(orders_data)
    
    transformer = OrderTransformer()
    transformed_df = transformer.transform(df)
    
    records = transformed_df.to_dict('records')
    ti.xcom_push(key='transformed_orders', value=records)
    
    logger.info(f"Transformed {len(records)} orders")
    return len(records)


def transform_customers(**context):
    """Transform customers data"""
    from src.processing.transformations import CustomerTransformer
    
    ti = context['ti']
    customers_data = ti.xcom_pull(key='customers_data', task_ids='extract.extract_customers')
    
    if not customers_data:
        logger.warning("No customers data to transform")
        return 0
    
    df = pd.DataFrame(customers_data)
    
    transformer = CustomerTransformer()
    transformed_df = transformer.transform(df)
    
    records = transformed_df.to_dict('records')
    ti.xcom_push(key='transformed_customers', value=records)
    
    logger.info(f"Transformed {len(records)} customers")
    return len(records)


def validate_data_quality(**context):
    """Run data quality checks"""
    from src.quality.data_quality_checks import DataQualityPipeline
    
    ti = context['ti']
    
    orders = ti.xcom_pull(key='transformed_orders', task_ids='transform.transform_orders')
    customers = ti.xcom_pull(key='transformed_customers', task_ids='transform.transform_customers')
    
    pipeline = DataQualityPipeline()
    
    quality_results = {}
    
    if orders:
        orders_df = pd.DataFrame(orders)
        orders_report = pipeline.check_orders(orders_df)
        quality_results['orders'] = {
            'score': orders_report.overall_score,
            'passed': orders_report.passed_checks,
            'failed': orders_report.failed_checks
        }
    
    if customers:
        customers_df = pd.DataFrame(customers)
        customers_report = pipeline.check_customers(customers_df)
        quality_results['customers'] = {
            'score': customers_report.overall_score,
            'passed': customers_report.passed_checks,
            'failed': customers_report.failed_checks
        }
    
    summary = pipeline.get_overall_summary()
    ti.xcom_push(key='quality_summary', value=summary)
    
    # Fail if there are critical issues
    if summary.get('has_critical_failures', False):
        raise ValueError(f"Critical quality failures detected: {summary['critical_failures']}")
    
    logger.info(f"Quality validation passed: {quality_results}")
    return quality_results


def decide_load_path(**context):
    """Decide if data should be loaded based on quality"""
    ti = context['ti']
    summary = ti.xcom_pull(key='quality_summary', task_ids='validate_quality')
    
    avg_score = summary.get('average_score', 0)
    
    if avg_score >= 0.9:
        return 'load.load_to_warehouse'
    elif avg_score >= 0.7:
        return 'load.load_with_quarantine'
    else:
        return 'quality_failed'


def load_to_warehouse(**context):
    """Load transformed data to data warehouse"""
    from src.warehouse.data_warehouse import DataWarehouse
    
    ti = context['ti']
    warehouse = DataWarehouse()
    
    # Load orders
    orders = ti.xcom_pull(key='transformed_orders', task_ids='transform.transform_orders')
    if orders:
        orders_df = pd.DataFrame(orders)
        warehouse.upsert_fact_table('fact_orders', orders_df, ['order_id'])
        logger.info(f"Loaded {len(orders)} orders to warehouse")
    
    # Load customers
    customers = ti.xcom_pull(key='transformed_customers', task_ids='transform.transform_customers')
    if customers:
        customers_df = pd.DataFrame(customers)
        warehouse.upsert_dimension('dim_customers', customers_df, ['customer_id'])
        logger.info(f"Loaded {len(customers)} customers to warehouse")
    
    return {'orders': len(orders) if orders else 0, 'customers': len(customers) if customers else 0}


def load_with_quarantine(**context):
    """Load data with problematic records quarantined"""
    from src.warehouse.data_warehouse import DataWarehouse
    
    ti = context['ti']
    warehouse = DataWarehouse()
    
    # Similar to load_to_warehouse but with quarantine logic
    orders = ti.xcom_pull(key='transformed_orders', task_ids='transform.transform_orders')
    if orders:
        orders_df = pd.DataFrame(orders)
        
        # Separate good and bad records
        # (This is simplified - in reality would use quality check results)
        good_records = orders_df[orders_df['total_amount'] > 0]
        bad_records = orders_df[orders_df['total_amount'] <= 0]
        
        warehouse.upsert_fact_table('fact_orders', good_records, ['order_id'])
        if len(bad_records) > 0:
            warehouse.insert_quarantine('quarantine_orders', bad_records, 'invalid_amount')
        
        logger.info(f"Loaded {len(good_records)} orders, quarantined {len(bad_records)}")


def update_aggregates(**context):
    """Update aggregate tables"""
    from src.warehouse.data_warehouse import DataWarehouse
    
    execution_date = context['execution_date']
    warehouse = DataWarehouse()
    
    # Update daily sales aggregate
    warehouse.refresh_aggregate('agg_daily_sales', execution_date)
    
    # Update customer metrics
    warehouse.refresh_aggregate('agg_customer_metrics', execution_date)
    
    logger.info("Aggregates updated successfully")


def send_notification(**context):
    """Send completion notification"""
    ti = context['ti']
    
    load_result = ti.xcom_pull(key='return_value', task_ids='load.load_to_warehouse')
    quality_summary = ti.xcom_pull(key='quality_summary', task_ids='validate_quality')
    
    message = f"""
    Daily ETL Pipeline Completed
    ============================
    Date: {context['execution_date'].strftime('%Y-%m-%d')}
    
    Records Processed:
    - Orders: {load_result.get('orders', 0) if load_result else 0}
    - Customers: {load_result.get('customers', 0) if load_result else 0}
    
    Quality Score: {quality_summary.get('average_score', 0):.2%}
    """
    
    logger.info(message)
    # In production, send via email/Slack/etc.


# Create DAG
with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for e-commerce data',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'daily', 'e-commerce'],
    max_active_runs=1,
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Extract tasks group
    with TaskGroup('extract', tooltip='Data extraction tasks') as extract_group:
        extract_orders_task = PythonOperator(
            task_id='extract_orders',
            python_callable=extract_orders,
        )
        
        extract_customers_task = PythonOperator(
            task_id='extract_customers',
            python_callable=extract_customers,
        )
        
        extract_products_task = PythonOperator(
            task_id='extract_products',
            python_callable=extract_products,
        )
    
    # Transform tasks group
    with TaskGroup('transform', tooltip='Data transformation tasks') as transform_group:
        transform_orders_task = PythonOperator(
            task_id='transform_orders',
            python_callable=transform_orders,
        )
        
        transform_customers_task = PythonOperator(
            task_id='transform_customers',
            python_callable=transform_customers,
        )
    
    # Quality validation
    validate_quality_task = PythonOperator(
        task_id='validate_quality',
        python_callable=validate_data_quality,
    )
    
    # Branch based on quality
    quality_branch = BranchPythonOperator(
        task_id='quality_branch',
        python_callable=decide_load_path,
    )
    
    # Load tasks group
    with TaskGroup('load', tooltip='Data loading tasks') as load_group:
        load_warehouse_task = PythonOperator(
            task_id='load_to_warehouse',
            python_callable=load_to_warehouse,
        )
        
        load_quarantine_task = PythonOperator(
            task_id='load_with_quarantine',
            python_callable=load_with_quarantine,
        )
    
    quality_failed = DummyOperator(task_id='quality_failed')
    
    # Update aggregates
    update_agg_task = PythonOperator(
        task_id='update_aggregates',
        python_callable=update_aggregates,
        trigger_rule='one_success',
    )
    
    # Send notification
    notify_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
        trigger_rule='all_done',
    )
    
    end = DummyOperator(task_id='end', trigger_rule='all_done')
    
    # Define task dependencies
    start >> extract_group >> transform_group >> validate_quality_task >> quality_branch
    quality_branch >> [load_warehouse_task, load_quarantine_task, quality_failed]
    [load_warehouse_task, load_quarantine_task] >> update_agg_task
    [update_agg_task, quality_failed] >> notify_task >> end
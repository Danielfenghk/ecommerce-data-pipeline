"""
Data Quality DAG for E-Commerce Pipeline
Runs comprehensive data quality checks on warehouse data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.quality.data_quality_checks import (
    DataQualityChecker,
    QualitySeverity,
    QualityCheckType
)
from src.utils.database_utils import DatabaseConnection
from src.utils.logging_utils import setup_logger
from src.config.settings import settings

logger = setup_logger('data_quality_dag')

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Create DAG
dag = DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Comprehensive data quality checks for warehouse',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['data-quality', 'warehouse', 'monitoring'],
)


def check_customers_quality(**context):
    """Run quality checks on customer dimension"""
    logger.info("Starting customer dimension quality checks")
    
    conn = DatabaseConnection(
        host=settings.WAREHOUSE_HOST,
        port=settings.WAREHOUSE_PORT,
        database=settings.WAREHOUSE_DB,
        user=settings.WAREHOUSE_USER,
        password=settings.WAREHOUSE_PASSWORD
    )
    
    try:
        conn.connect()
        
        # Fetch customer data
        customers_df = conn.fetch_dataframe("""
            SELECT * FROM dim_customers
            WHERE updated_at >= NOW() - INTERVAL '24 hours'
        """)
        
        if customers_df.empty:
            logger.warning("No recent customer data found")
            return {'status': 'warning', 'message': 'No recent data'}
        
        # Initialize quality checker
        checker = DataQualityChecker(customers_df, "dim_customers")
        
        # Run checks
        results = []
        
        # Completeness checks
        results.append(checker.check_not_null('customer_id', severity=QualitySeverity.CRITICAL))
        results.append(checker.check_not_null('email', severity=QualitySeverity.HIGH))
        results.append(checker.check_not_null('first_name', severity=QualitySeverity.MEDIUM))
        results.append(checker.check_not_null('last_name', severity=QualitySeverity.MEDIUM))
        
        # Uniqueness checks
        results.append(checker.check_unique('customer_id', severity=QualitySeverity.CRITICAL))
        results.append(checker.check_unique('email', severity=QualitySeverity.HIGH))
        
        # Format checks
        results.append(checker.check_regex(
            'email',
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            severity=QualitySeverity.MEDIUM
        ))
        
        # Range checks
        if 'age' in customers_df.columns:
            results.append(checker.check_range('age', 18, 120, severity=QualitySeverity.LOW))
        
        # Generate report
        report = checker.generate_report(results)
        
        # Store results
        context['ti'].xcom_push(key='customer_quality_report', value=report)
        
        # Check for critical failures
        critical_failures = [r for r in results if not r.passed and r.severity == 'critical']
        
        if critical_failures:
            raise ValueError(f"Critical quality failures: {len(critical_failures)}")
        
        logger.info(f"Customer quality check completed: {report['summary']}")
        return report
        
    finally:
        conn.disconnect()


def check_products_quality(**context):
    """Run quality checks on product dimension"""
    logger.info("Starting product dimension quality checks")
    
    conn = DatabaseConnection(
        host=settings.WAREHOUSE_HOST,
        port=settings.WAREHOUSE_PORT,
        database=settings.WAREHOUSE_DB,
        user=settings.WAREHOUSE_USER,
        password=settings.WAREHOUSE_PASSWORD
    )
    
    try:
        conn.connect()
        
        # Fetch product data
        products_df = conn.fetch_dataframe("""
            SELECT * FROM dim_products
            WHERE updated_at >= NOW() - INTERVAL '24 hours'
        """)
        
        if products_df.empty:
            logger.warning("No recent product data found")
            return {'status': 'warning', 'message': 'No recent data'}
        
        # Initialize quality checker
        checker = DataQualityChecker(products_df, "dim_products")
        
        # Run checks
        results = []
        
        # Completeness checks
        results.append(checker.check_not_null('product_id', severity=QualitySeverity.CRITICAL))
        results.append(checker.check_not_null('product_name', severity=QualitySeverity.HIGH))
        results.append(checker.check_not_null('category', severity=QualitySeverity.MEDIUM))
        results.append(checker.check_not_null('price', severity=QualitySeverity.HIGH))
        
        # Uniqueness checks
        results.append(checker.check_unique('product_id', severity=QualitySeverity.CRITICAL))
        results.append(checker.check_unique('sku', severity=QualitySeverity.HIGH))
        
        # Range checks
        results.append(checker.check_range('price', 0.01, 100000, severity=QualitySeverity.HIGH))
        results.append(checker.check_range('stock_quantity', 0, 1000000, severity=QualitySeverity.MEDIUM))
        
        # Valid values check
        if 'status' in products_df.columns:
            results.append(checker.check_valid_values(
                'status',
                ['active', 'inactive', 'discontinued'],
                severity=QualitySeverity.MEDIUM
            ))
        
        # Generate report
        report = checker.generate_report(results)
        
        # Store results
        context['ti'].xcom_push(key='product_quality_report', value=report)
        
        logger.info(f"Product quality check completed: {report['summary']}")
        return report
        
    finally:
        conn.disconnect()


def check_orders_quality(**context):
    """Run quality checks on orders fact table"""
    logger.info("Starting orders fact table quality checks")
    
    conn = DatabaseConnection(
        host=settings.WAREHOUSE_HOST,
        port=settings.WAREHOUSE_PORT,
        database=settings.WAREHOUSE_DB,
        user=settings.WAREHOUSE_USER,
        password=settings.WAREHOUSE_PASSWORD
    )
    
    try:
        conn.connect()
        
        # Fetch recent orders
        orders_df = conn.fetch_dataframe("""
            SELECT * FROM fact_orders
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)
        
        if orders_df.empty:
            logger.warning("No recent orders found")
            return {'status': 'warning', 'message': 'No recent data'}
        
        # Initialize quality checker
        checker = DataQualityChecker(orders_df, "fact_orders")
        
        # Run checks
        results = []
        
        # Completeness checks
        results.append(checker.check_not_null('order_id', severity=QualitySeverity.CRITICAL))
        results.append(checker.check_not_null('customer_id', severity=QualitySeverity.CRITICAL))
        results.append(checker.check_not_null('order_date', severity=QualitySeverity.HIGH))
        results.append(checker.check_not_null('total_amount', severity=QualitySeverity.HIGH))
        
        # Uniqueness checks
        results.append(checker.check_unique('order_id', severity=QualitySeverity.CRITICAL))
        
        # Range checks
        results.append(checker.check_range('total_amount', 0, 1000000, severity=QualitySeverity.HIGH))
        results.append(checker.check_range('quantity', 1, 10000, severity=QualitySeverity.MEDIUM))
        
        # Valid values check
        results.append(checker.check_valid_values(
            'order_status',
            ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'],
            severity=QualitySeverity.MEDIUM
        ))
        
        # Timeliness check
        results.append(checker.check_freshness(
            'created_at',
            max_age_hours=24,
            severity=QualitySeverity.HIGH
        ))
        
        # Generate report
        report = checker.generate_report(results)
        
        # Store results
        context['ti'].xcom_push(key='orders_quality_report', value=report)
        
        logger.info(f"Orders quality check completed: {report['summary']}")
        return report
        
    finally:
        conn.disconnect()


def check_referential_integrity(**context):
    """Check referential integrity between tables"""
    logger.info("Starting referential integrity checks")
    
    conn = DatabaseConnection(
        host=settings.WAREHOUSE_HOST,
        port=settings.WAREHOUSE_PORT,
        database=settings.WAREHOUSE_DB,
        user=settings.WAREHOUSE_USER,
        password=settings.WAREHOUSE_PASSWORD
    )
    
    try:
        conn.connect()
        
        results = []
        
        # Get valid customer IDs
        valid_customers = conn.fetch_dataframe(
            "SELECT customer_id FROM dim_customers"
        )['customer_id'].tolist()
        
        # Check orders reference valid customers
        orders_df = conn.fetch_dataframe(
            "SELECT DISTINCT customer_id FROM fact_orders"
        )
        
        invalid_customer_refs = orders_df[~orders_df['customer_id'].isin(valid_customers)]
        
        results.append({
            'check': 'orders_customer_fk',
            'passed': len(invalid_customer_refs) == 0,
            'invalid_count': len(invalid_customer_refs),
            'total_count': len(orders_df),
            'details': f"Found {len(invalid_customer_refs)} orders with invalid customer references"
        })
        
        # Get valid product IDs
        valid_products = conn.fetch_dataframe(
            "SELECT product_id FROM dim_products"
        )['product_id'].tolist()
        
        # Check order items reference valid products
        order_items_df = conn.fetch_dataframe(
            "SELECT DISTINCT product_id FROM fact_order_items"
        )
        
        invalid_product_refs = order_items_df[~order_items_df['product_id'].isin(valid_products)]
        
        results.append({
            'check': 'order_items_product_fk',
            'passed': len(invalid_product_refs) == 0,
            'invalid_count': len(invalid_product_refs),
            'total_count': len(order_items_df),
            'details': f"Found {len(invalid_product_refs)} order items with invalid product references"
        })
        
        # Check order items reference valid orders
        valid_orders = conn.fetch_dataframe(
            "SELECT order_id FROM fact_orders"
        )['order_id'].tolist()
        
        order_items_orders = conn.fetch_dataframe(
            "SELECT DISTINCT order_id FROM fact_order_items"
        )
        
        invalid_order_refs = order_items_orders[~order_items_orders['order_id'].isin(valid_orders)]
        
        results.append({
            'check': 'order_items_order_fk',
            'passed': len(invalid_order_refs) == 0,
            'invalid_count': len(invalid_order_refs),
            'total_count': len(order_items_orders),
            'details': f"Found {len(invalid_order_refs)} order items with invalid order references"
        })
        
        # Check date dimension references
        valid_dates = conn.fetch_dataframe(
            "SELECT date_key FROM dim_date"
        )['date_key'].tolist()
        
        orders_dates = conn.fetch_dataframe(
            "SELECT DISTINCT date_key FROM fact_orders WHERE date_key IS NOT NULL"
        )
        
        invalid_date_refs = orders_dates[~orders_dates['date_key'].isin(valid_dates)]
        
        results.append({
            'check': 'orders_date_fk',
            'passed': len(invalid_date_refs) == 0,
            'invalid_count': len(invalid_date_refs),
            'total_count': len(orders_dates),
            'details': f"Found {len(invalid_date_refs)} orders with invalid date references"
        })
        
        # Store results
        context['ti'].xcom_push(key='referential_integrity_results', value=results)
        
        # Check for failures
        failures = [r for r in results if not r['passed']]
        
        if failures:
            logger.error(f"Referential integrity failures: {failures}")
            raise ValueError(f"Found {len(failures)} referential integrity issues")
        
        logger.info("Referential integrity check completed successfully")
        return results
        
    finally:
        conn.disconnect()


def check_data_consistency(**context):
    """Check data consistency across tables"""
    logger.info("Starting data consistency checks")
    
    conn = DatabaseConnection(
        host=settings.WAREHOUSE_HOST,
        port=settings.WAREHOUSE_PORT,
        database=settings.WAREHOUSE_DB,
        user=settings.WAREHOUSE_USER,
        password=settings.WAREHOUSE_PASSWORD
    )
    
    try:
        conn.connect()
        
        results = []
        
        # Check order totals match sum of order items
        order_totals_check = conn.fetch_dataframe("""
            SELECT 
                o.order_id,
                o.total_amount as order_total,
                COALESCE(SUM(oi.quantity * oi.unit_price), 0) as calculated_total,
                ABS(o.total_amount - COALESCE(SUM(oi.quantity * oi.unit_price), 0)) as difference
            FROM fact_orders o
            LEFT JOIN fact_order_items oi ON o.order_id = oi.order_id
            GROUP BY o.order_id, o.total_amount
            HAVING ABS(o.total_amount - COALESCE(SUM(oi.quantity * oi.unit_price), 0)) > 0.01
        """)
        
        results.append({
            'check': 'order_totals_consistency',
            'passed': len(order_totals_check) == 0,
            'discrepancy_count': len(order_totals_check),
            'details': f"Found {len(order_totals_check)} orders with mismatched totals"
        })
        
        # Check customer order counts match
        customer_order_counts = conn.fetch_dataframe("""
            SELECT 
                c.customer_id,
                c.total_orders as stored_count,
                COUNT(o.order_id) as actual_count
            FROM dim_customers c
            LEFT JOIN fact_orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.total_orders
            HAVING c.total_orders != COUNT(o.order_id)
        """)
        
        results.append({
            'check': 'customer_order_count_consistency',
            'passed': len(customer_order_counts) == 0,
            'discrepancy_count': len(customer_order_counts),
            'details': f"Found {len(customer_order_counts)} customers with mismatched order counts"
        })
        
        # Check product stock doesn't go negative
        negative_stock = conn.fetch_dataframe("""
            SELECT product_id, product_name, stock_quantity
            FROM dim_products
            WHERE stock_quantity < 0
        """)
        
        results.append({
            'check': 'negative_stock_check',
            'passed': len(negative_stock) == 0,
            'count': len(negative_stock),
            'details': f"Found {len(negative_stock)} products with negative stock"
        })
        
        # Check for duplicate records in fact tables
        duplicate_orders = conn.fetch_dataframe("""
            SELECT order_id, COUNT(*) as count
            FROM fact_orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        """)
        
        results.append({
            'check': 'duplicate_orders_check',
            'passed': len(duplicate_orders) == 0,
            'count': len(duplicate_orders),
            'details': f"Found {len(duplicate_orders)} duplicate order IDs"
        })
        
        # Store results
        context['ti'].xcom_push(key='consistency_results', value=results)
        
        # Check for failures
        failures = [r for r in results if not r['passed']]
        
        if failures:
            logger.warning(f"Data consistency issues found: {failures}")
        
        logger.info("Data consistency check completed")
        return results
        
    finally:
        conn.disconnect()


def check_data_freshness(**context):
    """Check that data is being updated regularly"""
    logger.info("Starting data freshness checks")
    
    conn = DatabaseConnection(
        host=settings.WAREHOUSE_HOST,
        port=settings.WAREHOUSE_PORT,
        database=settings.WAREHOUSE_DB,
        user=settings.WAREHOUSE_USER,
        password=settings.WAREHOUSE_PASSWORD
    )
    
    try:
        conn.connect()
        
        results = []
        
        # Check last update time for each table
        tables_to_check = [
            ('dim_customers', 'updated_at', 24),
            ('dim_products', 'updated_at', 24),
            ('fact_orders', 'created_at', 4),
            ('fact_order_items', 'created_at', 4),
        ]
        
        for table, column, max_hours in tables_to_check:
            try:
                freshness = conn.fetch_dataframe(f"""
                    SELECT 
                        MAX({column}) as last_update,
                        EXTRACT(EPOCH FROM (NOW() - MAX({column}))) / 3600 as hours_since_update
                    FROM {table}
                """)
                
                if freshness.empty or freshness['last_update'].iloc[0] is None:
                    results.append({
                        'table': table,
                        'passed': False,
                        'hours_since_update': None,
                        'max_hours': max_hours,
                        'details': f"No data found in {table}"
                    })
                else:
                    hours_since = freshness['hours_since_update'].iloc[0]
                    passed = hours_since <= max_hours
                    
                    results.append({
                        'table': table,
                        'passed': passed,
                        'hours_since_update': round(hours_since, 2),
                        'max_hours': max_hours,
                        'details': f"Last update {round(hours_since, 2)} hours ago"
                    })
                    
            except Exception as e:
                results.append({
                    'table': table,
                    'passed': False,
                    'error': str(e),
                    'details': f"Error checking {table}: {str(e)}"
                })
        
        # Store results
        context['ti'].xcom_push(key='freshness_results', value=results)
        
        # Check for stale tables
        stale_tables = [r for r in results if not r['passed']]
        
        if stale_tables:
            logger.warning(f"Stale tables found: {stale_tables}")
        
        logger.info("Data freshness check completed")
        return results
        
    finally:
        conn.disconnect()


def aggregate_quality_results(**context):
    """Aggregate all quality check results"""
    ti = context['ti']
    
    # Collect all results
    customer_report = ti.xcom_pull(key='customer_quality_report', task_ids='check_customers_quality')
    product_report = ti.xcom_pull(key='product_quality_report', task_ids='check_products_quality')
    orders_report = ti.xcom_pull(key='orders_quality_report', task_ids='check_orders_quality')
    ri_results = ti.xcom_pull(key='referential_integrity_results', task_ids='check_referential_integrity')
    consistency_results = ti.xcom_pull(key='consistency_results', task_ids='check_data_consistency')
    freshness_results = ti.xcom_pull(key='freshness_results', task_ids='check_data_freshness')
    
    # Aggregate results
    aggregate = {
        'timestamp': datetime.now().isoformat(),
        'customer_quality': customer_report,
        'product_quality': product_report,
        'orders_quality': orders_report,
        'referential_integrity': ri_results,
        'data_consistency': consistency_results,
        'data_freshness': freshness_results,
        'overall_status': 'healthy'
    }
    
    # Determine overall status
    all_passed = True
    
    for report in [customer_report, product_report, orders_report]:
        if report and isinstance(report, dict):
            if report.get('summary', {}).get('failed_checks', 0) > 0:
                all_passed = False
    
    if ri_results:
        if any(not r.get('passed', True) for r in ri_results):
            all_passed = False
    
    if consistency_results:
        if any(not r.get('passed', True) for r in consistency_results):
            all_passed = False
    
    if freshness_results:
        if any(not r.get('passed', True) for r in freshness_results):
            all_passed = False
    
    aggregate['overall_status'] = 'healthy' if all_passed else 'degraded'
    
    # Store aggregate results
    conn = DatabaseConnection(
        host=settings.WAREHOUSE_HOST,
        port=settings.WAREHOUSE_PORT,
        database=settings.WAREHOUSE_DB,
        user=settings.WAREHOUSE_USER,
        password=settings.WAREHOUSE_PASSWORD
    )
    
    try:
        conn.connect()
        
        # Insert into quality metrics table
        import json
        conn.execute_query("""
            INSERT INTO data_quality_metrics (
                run_timestamp,
                overall_status,
                results_json,
                created_at
            ) VALUES (%s, %s, %s, NOW())
        """, (
            datetime.now(),
            aggregate['overall_status'],
            json.dumps(aggregate)
        ))
        
        logger.info(f"Quality results aggregated. Overall status: {aggregate['overall_status']}")
        
    finally:
        conn.disconnect()
    
    return aggregate


def decide_notification(**context):
    """Decide whether to send notification based on results"""
    ti = context['ti']
    aggregate = ti.xcom_pull(task_ids='aggregate_quality_results')
    
    if aggregate and aggregate.get('overall_status') == 'degraded':
        return 'send_alert_notification'
    else:
        return 'send_success_notification'


def send_alert(**context):
    """Send alert notification for quality issues"""
    ti = context['ti']
    aggregate = ti.xcom_pull(task_ids='aggregate_quality_results')
    
    # In production, this would send to Slack, PagerDuty, etc.
    logger.error(f"DATA QUALITY ALERT: {aggregate}")
    
    # You could also write to a monitoring table
    return "Alert sent"


def send_success(**context):
    """Send success notification"""
    logger.info("All data quality checks passed successfully")
    return "Success notification sent"


# Define tasks
start = EmptyOperator(task_id='start', dag=dag)

check_customers = PythonOperator(
    task_id='check_customers_quality',
    python_callable=check_customers_quality,
    dag=dag,
)

check_products = PythonOperator(
    task_id='check_products_quality',
    python_callable=check_products_quality,
    dag=dag,
)

check_orders = PythonOperator(
    task_id='check_orders_quality',
    python_callable=check_orders_quality,
    dag=dag,
)

check_ri = PythonOperator(
    task_id='check_referential_integrity',
    python_callable=check_referential_integrity,
    dag=dag,
)

check_consistency = PythonOperator(
    task_id='check_data_consistency',
    python_callable=check_data_consistency,
    dag=dag,
)

check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

aggregate_results = PythonOperator(
    task_id='aggregate_quality_results',
    python_callable=aggregate_quality_results,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

decide_branch = BranchPythonOperator(
    task_id='decide_notification',
    python_callable=decide_notification,
    dag=dag,
)

send_alert_notification = PythonOperator(
    task_id='send_alert_notification',
    python_callable=send_alert,
    dag=dag,
)

send_success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success,
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# Define task dependencies
start >> [check_customers, check_products, check_orders]
[check_customers, check_products, check_orders] >> check_ri
check_ri >> check_consistency
check_consistency >> check_freshness
check_freshness >> aggregate_results
aggregate_results >> decide_branch
decide_branch >> [send_alert_notification, send_success_notification]
[send_alert_notification, send_success_notification] >> end
#!/usr/bin/env python3
"""
Manual Data Quality Check Runner
Run this to execute all quality checks on the warehouse
"""

import os
import sys
import warnings
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

warnings.filterwarnings('ignore')

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

WAREHOUSE_DB = {
    'host': 'localhost',
    'port': 5433,
    'database': 'ecommerce_warehouse',
    'user': 'warehouse_user',
    'password': 'warehouse_pass'
}


# =============================================================================
# DATA QUALITY CHECKER
# =============================================================================

class DataQualityChecker:
    """Performs data quality checks on warehouse tables."""
    
    def __init__(self):
        self.conn = psycopg2.connect(**WAREHOUSE_DB)
        self.results = []
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    def close(self):
        if self.conn:
            self.conn.close()
    
    def fetch_dataframe(self, query: str) -> pd.DataFrame:
        """Execute query and return DataFrame."""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    
    def record_result(self, table: str, check_name: str, passed: bool, 
                     actual_value: float = None, threshold: float = None,
                     severity: str = 'HIGH', details: str = None):
        """Record a quality check result."""
        result = {
            'run_id': self.run_id,
            'table_name': table,
            'check_name': check_name,
            'passed': passed,
            'actual_value': actual_value,
            'threshold': threshold,
            'severity': severity,
            'details': details,
            'timestamp': datetime.now()
        }
        self.results.append(result)
        
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status} | {check_name}")
        if details and not passed:
            print(f"         └─ {details}")
    
    def save_results(self):
        """Save results to database."""
        if not self.results:
            return
        
        cur = self.conn.cursor()
        
        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_results (
                result_id SERIAL PRIMARY KEY,
                run_id VARCHAR(50),
                table_name VARCHAR(100),
                check_name VARCHAR(200),
                passed BOOLEAN,
                actual_value DECIMAL(15,4),
                threshold DECIMAL(15,4),
                severity VARCHAR(20),
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        for r in self.results:
            cur.execute("""
                INSERT INTO data_quality_results 
                    (run_id, table_name, check_name, passed, actual_value, 
                     threshold, severity, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r['run_id'], r['table_name'], r['check_name'], r['passed'],
                r['actual_value'], r['threshold'], r['severity'], r['details']
            ))
        
        self.conn.commit()
        cur.close()
        print(f"\n   📊 Saved {len(self.results)} results to data_quality_results table")
    
    # =========================================================================
    # COMPLETENESS CHECKS
    # =========================================================================
    
    def check_table_not_empty(self, table: str) -> bool:
        """Check that table has data."""
        cur = self.conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        cur.close()
        
        passed = count > 0
        self.record_result(
            table=table,
            check_name=f"table_not_empty",
            passed=passed,
            actual_value=count,
            threshold=1,
            severity='CRITICAL',
            details=f"Row count: {count}" if not passed else None
        )
        return passed
    
    def check_null_percentage(self, table: str, column: str, max_null_pct: float = 5.0) -> bool:
        """Check that null percentage is within threshold."""
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as nulls
            FROM {table}
        """)
        row = cur.fetchone()
        cur.close()
        
        total, nulls = row[0], row[1]
        null_pct = (nulls / total * 100) if total > 0 else 0
        
        passed = null_pct <= max_null_pct
        self.record_result(
            table=table,
            check_name=f"null_check_{column}",
            passed=passed,
            actual_value=round(null_pct, 2),
            threshold=max_null_pct,
            severity='HIGH',
            details=f"{nulls}/{total} rows ({null_pct:.1f}%) are NULL" if not passed else None
        )
        return passed
    
    # =========================================================================
    # ACCURACY CHECKS
    # =========================================================================
    
    def check_value_range(self, table: str, column: str, 
                         min_val: float = None, max_val: float = None) -> bool:
        """Check values are within expected range."""
        conditions = []
        if min_val is not None:
            conditions.append(f"{column} < {min_val}")
        if max_val is not None:
            conditions.append(f"{column} > {max_val}")
        
        if not conditions:
            return True
        
        where_clause = " OR ".join(conditions)
        
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT COUNT(*) FROM {table} 
            WHERE {column} IS NOT NULL AND ({where_clause})
        """)
        violations = cur.fetchone()[0]
        cur.close()
        
        passed = violations == 0
        self.record_result(
            table=table,
            check_name=f"range_check_{column}",
            passed=passed,
            actual_value=violations,
            threshold=0,
            severity='HIGH',
            details=f"{violations} values outside range [{min_val}, {max_val}]" if not passed else None
        )
        return passed
    
    def check_no_duplicates(self, table: str, column: str) -> bool:
        """Check for duplicate values in column."""
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT {column}, COUNT(*) as cnt 
            FROM {table} 
            WHERE {column} IS NOT NULL
            GROUP BY {column} 
            HAVING COUNT(*) > 1
        """)
        duplicates = cur.fetchall()
        cur.close()
        
        passed = len(duplicates) == 0
        self.record_result(
            table=table,
            check_name=f"duplicate_check_{column}",
            passed=passed,
            actual_value=len(duplicates),
            threshold=0,
            severity='MEDIUM',
            details=f"Found {len(duplicates)} duplicate values" if not passed else None
        )
        return passed
    
    # =========================================================================
    # CONSISTENCY CHECKS
    # =========================================================================
    
    def check_referential_integrity(self, fact_table: str, fact_column: str,
                                    dim_table: str, dim_column: str) -> bool:
        """Check foreign key references exist in dimension table."""
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT COUNT(*) FROM {fact_table} f
            WHERE f.{fact_column} IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM {dim_table} d 
                WHERE d.{dim_column} = f.{fact_column}
            )
        """)
        orphans = cur.fetchone()[0]
        cur.close()
        
        passed = orphans == 0
        self.record_result(
            table=fact_table,
            check_name=f"referential_integrity_{fact_column}",
            passed=passed,
            actual_value=orphans,
            threshold=0,
            severity='CRITICAL',
            details=f"{orphans} orphan records (FK not in {dim_table})" if not passed else None
        )
        return passed
    
    # =========================================================================
    # TIMELINESS CHECKS
    # =========================================================================
    
    def check_data_freshness(self, table: str, date_column: str, 
                            max_age_days: int = 7) -> bool:
        """Check that data is recent."""
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT MAX({date_column}) FROM {table}
        """)
        result = cur.fetchone()[0]
        cur.close()
        
        if result is None:
            self.record_result(
                table=table,
                check_name=f"freshness_{date_column}",
                passed=False,
                severity='HIGH',
                details="No data found"
            )
            return False
        
        # Handle both date and datetime
        if hasattr(result, 'date'):
            latest_date = result.date()
        else:
            latest_date = result
        
        age_days = (datetime.now().date() - latest_date).days
        passed = age_days <= max_age_days
        
        self.record_result(
            table=table,
            check_name=f"freshness_{date_column}",
            passed=passed,
            actual_value=age_days,
            threshold=max_age_days,
            severity='MEDIUM',
            details=f"Latest data is {age_days} days old" if not passed else None
        )
        return passed
    
    # =========================================================================
    # BUSINESS RULE CHECKS
    # =========================================================================
    
    def check_positive_amounts(self, table: str, column: str) -> bool:
        """Check that monetary amounts are positive."""
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT COUNT(*) FROM {table} 
            WHERE {column} IS NOT NULL AND {column} < 0
        """)
        negatives = cur.fetchone()[0]
        cur.close()
        
        passed = negatives == 0
        self.record_result(
            table=table,
            check_name=f"positive_amount_{column}",
            passed=passed,
            actual_value=negatives,
            threshold=0,
            severity='HIGH',
            details=f"{negatives} records have negative {column}" if not passed else None
        )
        return passed
    
    def check_valid_status(self, table: str, column: str, 
                          valid_values: List[str]) -> bool:
        """Check that status values are valid."""
        placeholders = ','.join([f"'{v}'" for v in valid_values])
        
        cur = self.conn.cursor()
        cur.execute(f"""
            SELECT {column}, COUNT(*) 
            FROM {table} 
            WHERE {column} IS NOT NULL 
            AND {column} NOT IN ({placeholders})
            GROUP BY {column}
        """)
        invalid = cur.fetchall()
        cur.close()
        
        passed = len(invalid) == 0
        details = None
        if not passed:
            invalid_vals = [f"{r[0]} ({r[1]})" for r in invalid]
            details = f"Invalid values: {', '.join(invalid_vals)}"
        
        self.record_result(
            table=table,
            check_name=f"valid_status_{column}",
            passed=passed,
            actual_value=len(invalid),
            threshold=0,
            severity='MEDIUM',
            details=details
        )
        return passed


# =============================================================================
# RUN ALL QUALITY CHECKS
# =============================================================================

def run_all_checks():
    """Run all data quality checks."""
    
    print("\n" + "=" * 70)
    print("🔍 DATA QUALITY CHECK SUITE")
    print("=" * 70)
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    checker = DataQualityChecker()
    all_passed = True
    
    try:
        # =====================================================================
        # DIM_CUSTOMERS CHECKS
        # =====================================================================
        print("\n📋 dim_customers")
        print("-" * 50)
        
        all_passed &= checker.check_table_not_empty('dim_customers')
        all_passed &= checker.check_null_percentage('dim_customers', 'customer_id', max_null_pct=0)
        all_passed &= checker.check_null_percentage('dim_customers', 'email', max_null_pct=5)
        all_passed &= checker.check_no_duplicates('dim_customers', 'customer_id')
        all_passed &= checker.check_valid_status('dim_customers', 'customer_segment', 
                                                 ['Premium', 'Regular', 'New', 'VIP'])
        
        # =====================================================================
        # DIM_PRODUCTS CHECKS
        # =====================================================================
        print("\n📋 dim_products")
        print("-" * 50)
        
        all_passed &= checker.check_table_not_empty('dim_products')
        all_passed &= checker.check_null_percentage('dim_products', 'product_id', max_null_pct=0)
        all_passed &= checker.check_null_percentage('dim_products', 'name', max_null_pct=0)
        all_passed &= checker.check_no_duplicates('dim_products', 'product_id')
        all_passed &= checker.check_value_range('dim_products', 'current_price', min_val=0)
        all_passed &= checker.check_value_range('dim_products', 'profit_margin', min_val=-100, max_val=100)
        
        # =====================================================================
        # FACT_ORDERS CHECKS
        # =====================================================================
        print("\n📋 fact_orders")
        print("-" * 50)
        
        all_passed &= checker.check_table_not_empty('fact_orders')
        all_passed &= checker.check_null_percentage('fact_orders', 'order_id', max_null_pct=0)
        all_passed &= checker.check_null_percentage('fact_orders', 'customer_key', max_null_pct=5)
        all_passed &= checker.check_no_duplicates('fact_orders', 'order_id')
        all_passed &= checker.check_positive_amounts('fact_orders', 'total_amount')
        all_passed &= checker.check_positive_amounts('fact_orders', 'subtotal')
        all_passed &= checker.check_value_range('fact_orders', 'total_amount', min_val=0, max_val=100000)
        all_passed &= checker.check_valid_status('fact_orders', 'order_status',
                                                 ['pending', 'processing', 'shipped', 'completed', 'cancelled'])
        
        # Referential integrity
        all_passed &= checker.check_referential_integrity(
            'fact_orders', 'customer_key', 'dim_customers', 'customer_key'
        )
        all_passed &= checker.check_referential_integrity(
            'fact_orders', 'date_key', 'dim_date', 'date_key'
        )
        
        # =====================================================================
        # FACT_DAILY_SALES CHECKS
        # =====================================================================
        print("\n📋 fact_daily_sales")
        print("-" * 50)
        
        all_passed &= checker.check_table_not_empty('fact_daily_sales')
        all_passed &= checker.check_positive_amounts('fact_daily_sales', 'total_revenue')
        all_passed &= checker.check_value_range('fact_daily_sales', 'total_orders', min_val=0)
        all_passed &= checker.check_no_duplicates('fact_daily_sales', 'date_key')
        
        # =====================================================================
        # DIM_DATE CHECKS
        # =====================================================================
        print("\n📋 dim_date")
        print("-" * 50)
        
        all_passed &= checker.check_table_not_empty('dim_date')
        all_passed &= checker.check_no_duplicates('dim_date', 'date_key')
        
        # =====================================================================
        # SAVE RESULTS
        # =====================================================================
        checker.save_results()
        
        # =====================================================================
        # SUMMARY
        # =====================================================================
        passed_count = sum(1 for r in checker.results if r['passed'])
        failed_count = sum(1 for r in checker.results if not r['passed'])
        total_count = len(checker.results)
        
        print("\n" + "=" * 70)
        print("📊 QUALITY CHECK SUMMARY")
        print("=" * 70)
        print(f"   ✅ Passed: {passed_count}/{total_count}")
        print(f"   ❌ Failed: {failed_count}/{total_count}")
        print(f"   📈 Score:  {passed_count/total_count*100:.1f}%")
        
        if all_passed:
            print("\n   🎉 All quality checks PASSED!")
        else:
            print("\n   ⚠️  Some quality checks FAILED!")
            print("\n   Failed checks:")
            for r in checker.results:
                if not r['passed']:
                    print(f"      • {r['table_name']}.{r['check_name']}: {r['details']}")
        
        print("=" * 70 + "\n")
        
        return all_passed
        
    except Exception as e:
        print(f"\n❌ Error running quality checks: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        checker.close()


# =============================================================================
# VIEW HISTORICAL RESULTS
# =============================================================================

def view_history(limit: int = 20):
    """View recent quality check results."""
    conn = psycopg2.connect(**WAREHOUSE_DB)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT run_id, table_name, check_name, passed, actual_value, threshold, severity, created_at
        FROM data_quality_results
        ORDER BY created_at DESC
        LIMIT %s
    """, (limit,))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    print("\n" + "=" * 90)
    print("📜 RECENT QUALITY CHECK RESULTS")
    print("=" * 90)
    
    if not rows:
        print("   No results found")
        return
    
    print(f"{'Run ID':<20} {'Table':<20} {'Check':<25} {'Status':<8} {'Severity':<10}")
    print("-" * 90)
    
    for row in rows:
        run_id, table, check, passed, actual, threshold, severity, created = row
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{run_id:<20} {table:<20} {check:<25} {status:<8} {severity:<10}")
    
    print("=" * 90 + "\n")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--history':
        view_history()
    else:
        success = run_all_checks()
        sys.exit(0 if success else 1)
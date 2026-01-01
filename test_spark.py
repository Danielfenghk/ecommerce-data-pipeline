#!/usr/bin/env python3
"""Test Spark Iceberg connection"""

from iceberg_config import get_spark_iceberg_session

try:
    spark = get_spark_iceberg_session()
    print("Spark session created successfully")

    # Try a simple SQL query
    result = spark.sql("SHOW DATABASES")
    result.show()

    print("Basic Spark operations completed successfully")

    spark.stop()
    print("Test completed successfully")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

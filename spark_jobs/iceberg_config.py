"""
Iceberg Configuration for Spark
"""

from pyspark.sql import SparkSession


def get_spark_iceberg_session(app_name: str = "IcebergETL") -> SparkSession:
    """
    Create Spark session with Iceberg support.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "software.amazon.awssdk:bundle:2.21.1,"
                "software.amazon.awssdk:url-connection-client:2.21.1") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "/home/iceberg/warehouse") \
        .getOrCreate()

    return spark
"""
Configuration settings for the E-Commerce Data Pipeline.
Loads environment variables and provides centralized configuration.
"""

import os
from dataclasses import dataclass, field
from typing import Optional, List
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def jdbc_url(self) -> str:
        """Generate JDBC connection string for Spark."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for psycopg2."""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str
    consumer_group: str
    topics: dict = field(default_factory=lambda: {
        'orders': 'ecommerce.orders',
        'customers': 'ecommerce.customers',
        'products': 'ecommerce.products',
        'events': 'ecommerce.events',
        'dlq': 'ecommerce.dead-letter-queue'
    })
    
    @property
    def bootstrap_servers_list(self) -> List[str]:
        """Return bootstrap servers as list."""
        return self.bootstrap_servers.split(',')


@dataclass
class MinIOConfig:
    """MinIO/S3 configuration."""
    endpoint: str
    access_key: str
    secret_key: str
    bucket_raw: str
    bucket_processed: str
    bucket_analytics: str = "analytics-data"
    secure: bool = False
    
    @property
    def endpoint_url(self) -> str:
        """Generate endpoint URL."""
        protocol = "https" if self.secure else "http"
        return f"{protocol}://{self.endpoint}"


@dataclass
class SparkConfig:
    """Spark configuration."""
    master_url: str
    app_name: str = "EcommerceDataPipeline"
    executor_memory: str = "2g"
    executor_cores: int = 2
    driver_memory: str = "1g"
    
    def get_spark_conf(self) -> dict:
        """Get Spark configuration as dictionary."""
        return {
            'spark.master': self.master_url,
            'spark.app.name': self.app_name,
            'spark.executor.memory': self.executor_memory,
            'spark.executor.cores': str(self.executor_cores),
            'spark.driver.memory': self.driver_memory,
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        }


@dataclass
class APIConfig:
    """API configuration."""
    base_url: str
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 5


@dataclass
class RedisConfig:
    """Redis configuration."""
    host: str
    port: int
    db: int = 0
    password: Optional[str] = None
    
    @property
    def url(self) -> str:
        """Generate Redis URL."""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class Settings:
    """Central settings class that loads all configurations."""
    
    def __init__(self):
        self.source_db = DatabaseConfig(
            host=os.getenv('SOURCE_DB_HOST', 'localhost'),
            port=int(os.getenv('SOURCE_DB_PORT', 5432)),
            database=os.getenv('SOURCE_DB_NAME', 'ecommerce_source'),
            user=os.getenv('SOURCE_DB_USER', 'source_user'),
            password=os.getenv('SOURCE_DB_PASSWORD', 'source_password')
        )
        
        self.warehouse_db = DatabaseConfig(
            host=os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
            port=int(os.getenv('WAREHOUSE_DB_PORT', 5433)),
            database=os.getenv('WAREHOUSE_DB_NAME', 'ecommerce_warehouse'),
            user=os.getenv('WAREHOUSE_DB_USER', 'warehouse_user'),
            password=os.getenv('WAREHOUSE_DB_PASSWORD', 'warehouse_password')
        )
        
        self.kafka = KafkaConfig(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
            consumer_group=os.getenv('KAFKA_CONSUMER_GROUP', 'ecommerce-pipeline')
        )
        
        self.minio = MinIOConfig(
            endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minio_admin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minio_password'),
            bucket_raw=os.getenv('MINIO_BUCKET_RAW', 'raw-data'),
            bucket_processed=os.getenv('MINIO_BUCKET_PROCESSED', 'processed-data')
        )
        
        self.spark = SparkConfig(
            master_url=os.getenv('SPARK_MASTER_URL', 'local[*]')
        )
        
        self.api = APIConfig(
            base_url=os.getenv('MOCK_API_URL', 'http://localhost:5000')
        )
        
        self.redis = RedisConfig(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379))
        )
        
        # Paths
        self.base_path = Path(__file__).parent.parent.parent
        self.data_path = self.base_path / 'data'
        self.logs_path = self.base_path / 'logs'
        
        # Create directories if they don't exist
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.logs_path.mkdir(parents=True, exist_ok=True)
    
    def validate(self) -> bool:
        """Validate all configurations."""
        # Add validation logic here
        return True


# Global settings instance
settings = Settings()
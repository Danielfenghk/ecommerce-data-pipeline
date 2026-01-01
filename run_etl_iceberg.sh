#!/bin/bash
# =============================================================================
# E-Commerce Data Lakehouse ETL Pipeline Runner (Linux/Mac)
# =============================================================================
# This script executes the complete Iceberg-based lakehouse ETL workflow
# including service startup, data pipeline execution, and Trino validation

set -e  # Exit on any error

echo ""
echo "======================================================"
echo "   E-Commerce Data Lakehouse - Iceberg ETL Runner"
echo "======================================================"
echo ""

# =============================================================================
# Step 1: Verify Required Files
# =============================================================================
echo "[1/4] Verifying required files..."
REQUIRED_FILES=(
    "scripts/start_iceberg.ps1"
    "run_lakehouse_etl.py"
    "docker-compose.yml"
    "docker-compose.iceberg.yml"
    "spark_jobs/iceberg_config.py"
    "spark_jobs/iceberg_bronze_ingestion.py"
    "spark_jobs/iceberg_silver_transform.py"
    "spark_jobs/iceberg_gold_aggregate.py"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Error: Required file '$file' not found!"
        exit 1
    fi
done
echo "✅ All required files verified"

# =============================================================================
# Step 2: Start Iceberg Services
# =============================================================================
echo ""
echo "[2/4] Starting Iceberg services..."
echo "Note: This step uses PowerShell script. On Linux/Mac, you may need to run:"
echo "      pwsh ./scripts/start_iceberg.ps1"
echo ""

if command -v pwsh &> /dev/null; then
    pwsh ./scripts/start_iceberg.ps1
elif command -v powershell &> /dev/null; then
    powershell ./scripts/start_iceberg.ps1
else
    echo "⚠️  PowerShell not found. Please run the startup script manually:"
    echo "   pwsh ./scripts/start_iceberg.ps1"
    echo ""
    read -p "Press Enter after starting services manually..."
fi

# =============================================================================
# Step 3: Execute ETL Pipeline
# =============================================================================
echo ""
echo "[3/4] Executing ETL Pipeline..."

# Wait for services to be ready
echo "Waiting for Spark-Iceberg service to be ready..."
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
    if docker ps --filter "name=spark-iceberg" --filter "status=running" | grep -q spark-iceberg; then
        echo "✅ Spark-Iceberg service is running"
        break
    fi
    echo "   Attempt $attempt/$max_attempts: Waiting for Spark-Iceberg..."
    sleep 5
    ((attempt++))
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ Error: Spark-Iceberg service failed to start"
    exit 1
fi

# Execute the ETL pipeline
echo "Running ETL pipeline..."
docker exec spark-iceberg python /home/iceberg/spark-jobs/run_lakehouse_etl.py

echo "✅ ETL Pipeline completed successfully"

# =============================================================================
# Step 4: Validate with Trino
# =============================================================================
echo ""
echo "[4/4] Validating with Trino..."

# Wait for Trino to be ready
echo "Waiting for Trino service to be ready..."
attempt=1
while [ $attempt -le $max_attempts ]; do
    if docker ps --filter "name=trino" --filter "status=running" | grep -q trino; then
        echo "✅ Trino service is running"
        break
    fi
    echo "   Attempt $attempt/$max_attempts: Waiting for Trino..."
    sleep 5
    ((attempt++))
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ Error: Trino service failed to start"
    exit 1
fi

# Test Trino connectivity
echo "Testing Trino connectivity..."
docker exec trino trino --execute "SHOW CATALOGS" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Trino is working correctly"
else
    echo "⚠️  Trino connection test failed, but service may still be initializing"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "======================================================"
echo "   🎉 ETL Pipeline Execution Complete!"
echo "======================================================"
echo ""
echo "Service URLs:"
echo "   Nessie Catalog:   http://localhost:19120"
echo "   Jupyter Notebook: http://localhost:8888"
echo "   Trino UI:         http://localhost:8090"
echo "   Spark UI:         http://localhost:8180"
echo "   MinIO Console:    http://localhost:9001"
echo ""
echo "Next steps:"
echo "   1. Open Jupyter: http://localhost:8888"
echo "   2. Query data with Trino:"
echo "      docker exec trino trino"
echo "   3. Check MinIO: http://localhost:9001 (admin/minioadmin123)"
echo ""
echo "======================================================"

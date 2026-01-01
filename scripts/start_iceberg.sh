#!/bin/bash
# =============================================================================
# Start Iceberg Lakehouse Services (Linux/Mac/WSL)
# =============================================================================

set -e

echo ""
echo "======================================================"
echo "   E-Commerce Data Lakehouse - Iceberg Setup"
echo "======================================================"
echo ""

# Check if in project directory
if [ ! -f "docker-compose.yml" ]; then
    echo "Error: Please run from project root directory"
    exit 1
fi

# Step 1: Create directories
echo "[1/6] Creating directories..."
mkdir -p catalog/iceberg
mkdir -p trino/etc
mkdir -p trino/catalog
mkdir -p lakehouse/notebooks
mkdir -p src/lakehouse
echo "      Done"

# Step 2: Ensure network exists
echo "[2/6] Checking Docker network..."
docker network create pipeline-network 2>/dev/null || true
echo "      Done"

# Step 3: Start base services
echo "[3/6] Starting base services..."
docker-compose up -d postgres-source minio zookeeper kafka
sleep 10
echo "      Done"

# Step 4: Start Iceberg services
echo "[4/6] Starting Iceberg services..."
docker-compose -f docker-compose.yml -f docker-compose.iceberg.yml up -d
sleep 20
echo "      Done"

# Step 5: Wait for services
echo "[5/6] Waiting for services to be ready..."
max_wait=60
waited=0

while [ $waited -lt $max_wait ]; do
    if curl -s http://localhost:19120/api/v1/config > /dev/null 2>&1; then
        echo "      Nessie is ready"
        break
    fi
    sleep 5
    waited=$((waited + 5))
    echo "      Waiting... ($waited/$max_wait seconds)"
done

# Step 6: Show status
echo "[6/6] Checking service status..."
echo ""

for service in nessie spark-iceberg trino minio; do
    if docker ps --filter "name=$service" --format "{{.Status}}" | grep -q "Up"; then
        echo "   [OK] $service is running"
    else
        echo "   [X]  $service is not running"
    fi
done

echo ""
echo "======================================================"
echo "   Service URLs"
echo "======================================================"
echo ""
echo "   Nessie Catalog:   http://localhost:19120"
echo "   Jupyter Notebook: http://localhost:8888"
echo "   Trino UI:         http://localhost:8090"
echo "   Spark UI:         http://localhost:8180"
echo "   MinIO Console:    http://localhost:9001"
echo ""
echo "======================================================"
echo "   Next Steps"
echo "======================================================"
echo ""
echo "   1. Open Jupyter: http://localhost:8888"
echo "   2. Run ETL pipeline:"
echo "      docker exec spark-iceberg python /home/iceberg/spark-jobs/run_lakehouse_etl.py"
echo "   3. Query with Trino:"
echo "      docker exec -it trino trino"
echo ""
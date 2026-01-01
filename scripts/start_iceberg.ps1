# =============================================================================
# Start Iceberg Lakehouse Services (Windows PowerShell)
# =============================================================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "   E-Commerce Data Lakehouse - Iceberg Setup" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# Check if in project directory
if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "Error: Please run from project root directory" -ForegroundColor Red
    exit 1
}

# Step 1: Create directories
Write-Host "[1/6] Creating directories..." -ForegroundColor Yellow
$dirs = @(
    "catalog/iceberg",
    "trino/etc",
    "trino/catalog",
    "lakehouse/notebooks",
    "src/lakehouse"
)
foreach ($dir in $dirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}
Write-Host "      Done" -ForegroundColor Green

# Step 2: Ensure network exists
Write-Host "[2/6] Checking Docker network..." -ForegroundColor Yellow
$network = docker network ls --filter "name=pipeline-network" --format "{{.Name}}" 2>$null
if (-not $network) {
    docker network create pipeline-network
    Write-Host "      Created pipeline-network" -ForegroundColor Green
} else {
    Write-Host "      Network exists" -ForegroundColor Green
}

# Step 3: Start base services
Write-Host "[3/6] Starting base services..." -ForegroundColor Yellow
docker-compose up -d postgres-source minio zookeeper kafka 2>$null
Start-Sleep -Seconds 10
Write-Host "      Done" -ForegroundColor Green

# Step 4: Start Iceberg services
Write-Host "[4/6] Starting Iceberg services..." -ForegroundColor Yellow
docker-compose -f docker-compose.yml -f docker-compose.iceberg.yml up -d
Start-Sleep -Seconds 20
Write-Host "      Done" -ForegroundColor Green

# Step 5: Wait for services
Write-Host "[5/6] Waiting for services to be ready..." -ForegroundColor Yellow
$maxWait = 60
$waited = 0

while ($waited -lt $maxWait) {
    try {
        $nessieStatus = Invoke-RestMethod -Uri "http://localhost:19120/api/v1/config" -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($nessieStatus) {
            Write-Host "      Nessie is ready" -ForegroundColor Green
            break
        }
    } catch {
        Start-Sleep -Seconds 5
        $waited += 5
        Write-Host "      Waiting... ($waited/$maxWait seconds)" -ForegroundColor Gray
    }
}

# Step 6: Show status
Write-Host "[6/6] Checking service status..." -ForegroundColor Yellow
Write-Host ""

$services = @(
    @{Name="nessie"; Port=19120; Desc="Iceberg Catalog"},
    @{Name="spark-iceberg"; Port=8888; Desc="Jupyter Notebook"},
    @{Name="trino"; Port=8090; Desc="SQL Query Engine"},
    @{Name="minio"; Port=9001; Desc="Object Storage"}
)

foreach ($svc in $services) {
    $running = docker ps --filter "name=$($svc.Name)" --format "{{.Status}}" 2>$null
    if ($running -match "Up") {
        Write-Host "   [OK] $($svc.Desc): http://localhost:$($svc.Port)" -ForegroundColor Green
    } else {
        Write-Host "   [X]  $($svc.Desc): Not running" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "   Service URLs" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "   Nessie Catalog:   http://localhost:19120" -ForegroundColor White
Write-Host "   Jupyter Notebook: http://localhost:8888" -ForegroundColor White
Write-Host "   Trino UI:         http://localhost:8090" -ForegroundColor White
Write-Host "   Spark UI:         http://localhost:8180" -ForegroundColor White
Write-Host "   MinIO Console:    http://localhost:9001" -ForegroundColor White
Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "   Next Steps" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "   1. Open Jupyter: http://localhost:8888" -ForegroundColor Yellow
Write-Host "   2. Run ETL pipeline:" -ForegroundColor Yellow
Write-Host "      docker exec spark-iceberg python /home/iceberg/spark-jobs/run_lakehouse_etl.py" -ForegroundColor Gray
Write-Host "   3. Query with Trino:" -ForegroundColor Yellow
Write-Host "      docker exec -it trino trino" -ForegroundColor Gray
Write-Host ""
#!/bin/bash

# =============================================================================
# E-Commerce Data Pipeline - Start Script
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"
ENV_FILE="${PROJECT_ROOT}/.env"

# Print banner
print_banner() {
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                                                               ║${NC}"
    echo -e "${CYAN}║       ${GREEN}E-Commerce Data Pipeline Platform${CYAN}                      ║${NC}"
    echo -e "${CYAN}║                                                               ║${NC}"
    echo -e "${CYAN}║       ${YELLOW}Scalable ETL with Spark, Kafka & Airflow${CYAN}               ║${NC}"
    echo -e "${CYAN}║                                                               ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# Print section header
print_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# Log functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_section "Checking Prerequisites"

    local missing_deps=()

    # Check Docker
    if ! command -v docker &> /dev/null; then
        missing_deps+=("docker")
    else
        log_info "Docker: $(docker --version)"
    fi

    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        missing_deps+=("docker-compose")
    else
        if docker compose version &> /dev/null; then
            log_info "Docker Compose: $(docker compose version --short)"
        else
            log_info "Docker Compose: $(docker-compose --version)"
        fi
    fi

    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi

    # Check for missing dependencies
    if [ ${#missing_deps[@]} -gt 0 ]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        log_error "Please install the missing dependencies and try again."
        exit 1
    fi

    log_info "All prerequisites satisfied!"
}

# Setup environment
setup_environment() {
    print_section "Setting Up Environment"

    # Create .env file if it doesn't exist
    if [ ! -f "$ENV_FILE" ]; then
        if [ -f "${PROJECT_ROOT}/.env.example" ]; then
            log_step "Creating .env file from .env.example..."
            cp "${PROJECT_ROOT}/.env.example" "$ENV_FILE"
            log_info ".env file created successfully"
        else
            log_warn ".env.example not found, creating default .env file..."
            create_default_env
        fi
    else
        log_info ".env file already exists"
    fi

    # Create required directories
    log_step "Creating required directories..."
    mkdir -p "${PROJECT_ROOT}/data/sample"
    mkdir -p "${PROJECT_ROOT}/data/raw"
    mkdir -p "${PROJECT_ROOT}/data/processed"
    mkdir -p "${PROJECT_ROOT}/logs"
    mkdir -p "${PROJECT_ROOT}/dags"
    mkdir -p "${PROJECT_ROOT}/spark_jobs"

    # Set permissions for Airflow
    chmod -R 755 "${PROJECT_ROOT}/dags" 2>/dev/null || true
    chmod -R 755 "${PROJECT_ROOT}/logs" 2>/dev/null || true

    log_info "Environment setup complete!"
}

# Create default .env file
create_default_env() {
    cat > "$ENV_FILE" << 'EOF'
# =============================================================================
# E-Commerce Data Pipeline - Environment Configuration
# =============================================================================

# PostgreSQL Source Database
SOURCE_DB_HOST=postgres-source
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=ecommerce_source
SOURCE_DB_USER=ecommerce_user
SOURCE_DB_PASSWORD=ecommerce_pass

# PostgreSQL Data Warehouse
WAREHOUSE_DB_HOST=postgres-warehouse
WAREHOUSE_DB_PORT=5432
WAREHOUSE_DB_NAME=ecommerce_warehouse
WAREHOUSE_DB_USER=warehouse_user
WAREHOUSE_DB_PASSWORD=warehouse_pass

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181

# MinIO (S3 Compatible) Configuration
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_BUCKET=ecommerce-data-lake

# Apache Spark Configuration
SPARK_MASTER=spark://spark-master:7077
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g

# Apache Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Redis (Airflow Backend)
REDIS_HOST=redis
REDIS_PORT=6379

# Grafana Configuration
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin

# Environment
ENVIRONMENT=development
DEBUG=true
EOF
    log_info "Default .env file created"
}

# Create sample data
create_sample_data() {
    print_section "Creating Sample Data"

    local sample_dir="${PROJECT_ROOT}/data/sample"

    # Create products.csv
    log_step "Creating products.csv..."
    cat > "${sample_dir}/products.csv" << 'EOF'
product_id,name,category,subcategory,brand,price,cost,stock_quantity,description,created_at,updated_at
PROD001,Wireless Bluetooth Headphones,Electronics,Audio,SoundMax,79.99,45.00,150,Premium wireless headphones with noise cancellation,2024-01-01 10:00:00,2024-01-15 14:30:00
PROD002,Organic Green Tea,Food & Beverages,Tea,NatureBrew,12.99,6.50,500,100% organic green tea leaves from Japan,2024-01-02 11:00:00,2024-01-20 09:15:00
PROD003,Running Shoes Pro,Sports & Outdoors,Footwear,SpeedRunner,129.99,75.00,200,Professional running shoes with cushioned sole,2024-01-03 09:30:00,2024-01-18 16:45:00
PROD004,Stainless Steel Water Bottle,Home & Kitchen,Drinkware,EcoLife,24.99,12.00,300,Insulated water bottle keeps drinks cold for 24 hours,2024-01-04 14:00:00,2024-01-22 11:00:00
PROD005,Yoga Mat Premium,Sports & Outdoors,Fitness,ZenFit,39.99,18.00,250,Non-slip yoga mat with carrying strap,2024-01-05 08:00:00,2024-01-25 13:30:00
PROD006,Smart Watch Series 5,Electronics,Wearables,TechTime,299.99,180.00,100,Feature-rich smartwatch with health monitoring,2024-01-06 12:00:00,2024-01-28 10:00:00
PROD007,Organic Honey,Food & Beverages,Sweeteners,BeeNatural,18.99,9.00,400,Pure organic honey from local beekeepers,2024-01-07 10:30:00,2024-01-30 15:00:00
PROD008,Laptop Stand Adjustable,Electronics,Accessories,ErgoTech,49.99,25.00,175,Ergonomic laptop stand with adjustable height,2024-01-08 15:00:00,2024-02-01 12:00:00
PROD009,Cotton T-Shirt Classic,Clothing,Tops,ComfortWear,19.99,8.00,600,100% cotton classic fit t-shirt,2024-01-09 09:00:00,2024-02-03 14:00:00
PROD010,Vitamin D3 Supplements,Health & Wellness,Vitamins,VitaHealth,15.99,7.50,350,High-potency vitamin D3 supplements,2024-01-10 11:30:00,2024-02-05 09:30:00
PROD011,Ceramic Coffee Mug,Home & Kitchen,Drinkware,ArtisanCraft,14.99,6.00,450,Handcrafted ceramic coffee mug,2024-01-11 13:00:00,2024-02-07 16:00:00
PROD012,Wireless Mouse,Electronics,Accessories,ClickPro,29.99,15.00,280,Ergonomic wireless mouse with long battery life,2024-01-12 10:00:00,2024-02-10 11:30:00
PROD013,Protein Powder Vanilla,Health & Wellness,Supplements,FitFuel,44.99,22.00,220,Whey protein powder with natural vanilla flavor,2024-01-13 08:30:00,2024-02-12 13:00:00
PROD014,LED Desk Lamp,Home & Kitchen,Lighting,BrightLife,34.99,17.00,190,Adjustable LED desk lamp with multiple brightness levels,2024-01-14 14:30:00,2024-02-15 10:00:00
PROD015,Backpack Travel Pro,Sports & Outdoors,Bags,AdventureGear,89.99,45.00,130,Durable travel backpack with laptop compartment,2024-01-15 11:00:00,2024-02-18 15:30:00
EOF

    # Create customers.csv
    log_step "Creating customers.csv..."
    cat > "${sample_dir}/customers.csv" << 'EOF'
customer_id,first_name,last_name,email,phone,address,city,state,country,postal_code,registration_date,last_login,customer_segment,total_orders,total_spent
CUST001,John,Smith,john.smith@email.com,+1-555-0101,123 Main St,New York,NY,USA,10001,2023-06-15 10:30:00,2024-01-20 14:00:00,Premium,25,2450.75
CUST002,Emily,Johnson,emily.j@email.com,+1-555-0102,456 Oak Ave,Los Angeles,CA,USA,90001,2023-07-20 14:45:00,2024-01-22 09:30:00,Regular,12,890.50
CUST003,Michael,Williams,m.williams@email.com,+1-555-0103,789 Pine Rd,Chicago,IL,USA,60601,2023-08-10 09:00:00,2024-01-25 16:15:00,Premium,30,3200.00
CUST004,Sarah,Brown,sarah.brown@email.com,+1-555-0104,321 Elm St,Houston,TX,USA,77001,2023-09-05 11:20:00,2024-01-28 11:00:00,Regular,8,520.25
CUST005,David,Jones,david.jones@email.com,+1-555-0105,654 Maple Dr,Phoenix,AZ,USA,85001,2023-10-12 15:00:00,2024-01-30 13:45:00,New,3,175.00
CUST006,Jessica,Garcia,jgarcia@email.com,+1-555-0106,987 Cedar Ln,Philadelphia,PA,USA,19101,2023-11-08 08:30:00,2024-02-01 10:30:00,Regular,15,1100.50
CUST007,Robert,Miller,r.miller@email.com,+1-555-0107,147 Birch Way,San Antonio,TX,USA,78201,2023-12-01 12:15:00,2024-02-03 15:00:00,New,5,350.75
CUST008,Jennifer,Davis,jdavis@email.com,+1-555-0108,258 Walnut St,San Diego,CA,USA,92101,2024-01-05 10:00:00,2024-02-05 12:30:00,Premium,22,2800.00
CUST009,William,Rodriguez,wrodriguez@email.com,+1-555-0109,369 Spruce Ave,Dallas,TX,USA,75201,2024-01-10 14:30:00,2024-02-08 09:00:00,Regular,10,750.25
CUST010,Amanda,Martinez,amartinez@email.com,+1-555-0110,741 Ash Blvd,San Jose,CA,USA,95101,2024-01-15 09:45:00,2024-02-10 14:15:00,New,2,125.50
EOF

    # Create orders.json
    log_step "Creating orders.json..."
    cat > "${sample_dir}/orders.json" << 'EOF'
[
    {
        "order_id": "ORD-2024-00001",
        "customer_id": "CUST001",
        "order_date": "2024-01-15T10:30:00Z",
        "status": "completed",
        "payment_method": "credit_card",
        "shipping_address": "123 Main St, New York, NY 10001",
        "items": [
            {"product_id": "PROD001", "quantity": 1, "unit_price": 79.99, "discount": 0},
            {"product_id": "PROD004", "quantity": 2, "unit_price": 24.99, "discount": 5.00}
        ],
        "subtotal": 129.97,
        "tax": 10.40,
        "shipping_cost": 5.99,
        "total_amount": 146.36
    },
    {
        "order_id": "ORD-2024-00002",
        "customer_id": "CUST003",
        "order_date": "2024-01-16T14:45:00Z",
        "status": "completed",
        "payment_method": "paypal",
        "shipping_address": "789 Pine Rd, Chicago, IL 60601",
        "items": [
            {"product_id": "PROD006", "quantity": 1, "unit_price": 299.99, "discount": 30.00}
        ],
        "subtotal": 269.99,
        "tax": 21.60,
        "shipping_cost": 0,
        "total_amount": 291.59
    },
    {
        "order_id": "ORD-2024-00003",
        "customer_id": "CUST002",
        "order_date": "2024-01-17T09:15:00Z",
        "status": "shipped",
        "payment_method": "credit_card",
        "shipping_address": "456 Oak Ave, Los Angeles, CA 90001",
        "items": [
            {"product_id": "PROD003", "quantity": 1, "unit_price": 129.99, "discount": 0},
            {"product_id": "PROD005", "quantity": 1, "unit_price": 39.99, "discount": 0}
        ],
        "subtotal": 169.98,
        "tax": 13.60,
        "shipping_cost": 7.99,
        "total_amount": 191.57
    },
    {
        "order_id": "ORD-2024-00004",
        "customer_id": "CUST005",
        "order_date": "2024-01-18T16:00:00Z",
        "status": "processing",
        "payment_method": "debit_card",
        "shipping_address": "654 Maple Dr, Phoenix, AZ 85001",
        "items": [
            {"product_id": "PROD002", "quantity": 3, "unit_price": 12.99, "discount": 0},
            {"product_id": "PROD007", "quantity": 2, "unit_price": 18.99, "discount": 0}
        ],
        "subtotal": 76.95,
        "tax": 6.16,
        "shipping_cost": 5.99,
        "total_amount": 89.10
    },
    {
        "order_id": "ORD-2024-00005",
        "customer_id": "CUST008",
        "order_date": "2024-01-19T11:30:00Z",
        "status": "completed",
        "payment_method": "credit_card",
        "shipping_address": "258 Walnut St, San Diego, CA 92101",
        "items": [
            {"product_id": "PROD008", "quantity": 1, "unit_price": 49.99, "discount": 5.00},
            {"product_id": "PROD012", "quantity": 1, "unit_price": 29.99, "discount": 0},
            {"product_id": "PROD014", "quantity": 1, "unit_price": 34.99, "discount": 0}
        ],
        "subtotal": 109.97,
        "tax": 8.80,
        "shipping_cost": 0,
        "total_amount": 118.77
    },
    {
        "order_id": "ORD-2024-00006",
        "customer_id": "CUST004",
        "order_date": "2024-01-20T08:45:00Z",
        "status": "completed",
        "payment_method": "paypal",
        "shipping_address": "321 Elm St, Houston, TX 77001",
        "items": [
            {"product_id": "PROD009", "quantity": 4, "unit_price": 19.99, "discount": 8.00}
        ],
        "subtotal": 71.96,
        "tax": 5.76,
        "shipping_cost": 5.99,
        "total_amount": 83.71
    },
    {
        "order_id": "ORD-2024-00007",
        "customer_id": "CUST006",
        "order_date": "2024-01-21T13:20:00Z",
        "status": "shipped",
        "payment_method": "credit_card",
        "shipping_address": "987 Cedar Ln, Philadelphia, PA 19101",
        "items": [
            {"product_id": "PROD010", "quantity": 2, "unit_price": 15.99, "discount": 0},
            {"product_id": "PROD013", "quantity": 1, "unit_price": 44.99, "discount": 4.50}
        ],
        "subtotal": 72.47,
        "tax": 5.80,
        "shipping_cost": 5.99,
        "total_amount": 84.26
    },
    {
        "order_id": "ORD-2024-00008",
        "customer_id": "CUST007",
        "order_date": "2024-01-22T15:50:00Z",
        "status": "pending",
        "payment_method": "debit_card",
        "shipping_address": "147 Birch Way, San Antonio, TX 78201",
        "items": [
            {"product_id": "PROD015", "quantity": 1, "unit_price": 89.99, "discount": 10.00}
        ],
        "subtotal": 79.99,
        "tax": 6.40,
        "shipping_cost": 0,
        "total_amount": 86.39
    },
    {
        "order_id": "ORD-2024-00009",
        "customer_id": "CUST009",
        "order_date": "2024-01-23T10:00:00Z",
        "status": "completed",
        "payment_method": "credit_card",
        "shipping_address": "369 Spruce Ave, Dallas, TX 75201",
        "items": [
            {"product_id": "PROD011", "quantity": 3, "unit_price": 14.99, "discount": 0},
            {"product_id": "PROD002", "quantity": 2, "unit_price": 12.99, "discount": 0}
        ],
        "subtotal": 70.95,
        "tax": 5.68,
        "shipping_cost": 5.99,
        "total_amount": 82.62
    },
    {
        "order_id": "ORD-2024-00010",
        "customer_id": "CUST010",
        "order_date": "2024-01-24T12:30:00Z",
        "status": "processing",
        "payment_method": "paypal",
        "shipping_address": "741 Ash Blvd, San Jose, CA 95101",
        "items": [
            {"product_id": "PROD001", "quantity": 1, "unit_price": 79.99, "discount": 8.00}
        ],
        "subtotal": 71.99,
        "tax": 5.76,
        "shipping_cost": 5.99,
        "total_amount": 83.74
    }
]
EOF

    log_info "Sample data created successfully!"
}

# Pull Docker images
pull_images() {
    print_section "Pulling Docker Images"

    log_step "This may take a few minutes..."

    docker-compose -f "$COMPOSE_FILE" pull 2>&1 | while read -r line; do
        echo -e "  ${CYAN}>${NC} $line"
    done

    log_info "Docker images pulled successfully!"
}

# Start services
start_services() {
    print_section "Starting Services"

    log_step "Starting Docker containers..."

    docker-compose -f "$COMPOSE_FILE" up -d

    log_info "Waiting for services to initialize..."

    # Wait for services
    local services=("postgres-source" "postgres-warehouse" "kafka" "zookeeper" "minio" "spark-master")

    for service in "${services[@]}"; do
        log_step "Waiting for $service..."
        local retries=30
        while [ $retries -gt 0 ]; do
            if docker-compose -f "$COMPOSE_FILE" ps | grep -q "$service.*Up"; then
                log_info "$service is up!"
                break
            fi
            retries=$((retries - 1))
            sleep 2
        done

        if [ $retries -eq 0 ]; then
            log_warn "$service may not be fully started"
        fi
    done

    log_info "All services started!"
}

# Initialize databases
init_databases() {
    print_section "Initializing Databases"

    log_step "Waiting for PostgreSQL to be ready..."
    sleep 10

    # Initialize source database
    log_step "Initializing source database..."
    if [ -f "${PROJECT_ROOT}/sql/create_source_tables.sql" ]; then
        docker-compose -f "$COMPOSE_FILE" exec -T postgres-source \
            psql -U ecommerce_user -d ecommerce_source \
            -f /docker-entrypoint-initdb.d/create_source_tables.sql 2>/dev/null || true
    fi

    # Initialize warehouse database
    log_step "Initializing warehouse database..."
    if [ -f "${PROJECT_ROOT}/sql/create_warehouse_tables.sql" ]; then
        docker-compose -f "$COMPOSE_FILE" exec -T postgres-warehouse \
            psql -U warehouse_user -d ecommerce_warehouse \
            -f /docker-entrypoint-initdb.d/create_warehouse_tables.sql 2>/dev/null || true
    fi

    log_info "Databases initialized!"
}

# Create Kafka topics
create_kafka_topics() {
    print_section "Creating Kafka Topics"

    log_step "Waiting for Kafka to be ready..."
    sleep 15

    local topics=("orders" "products" "customers" "events" "processed-orders" "data-quality-alerts")

    for topic in "${topics[@]}"; do
        log_step "Creating topic: $topic"
        docker-compose -f "$COMPOSE_FILE" exec -T kafka \
            kafka-topics --create \
            --bootstrap-server localhost:9092 \
            --replication-factor 1 \
            --partitions 3 \
            --topic "$topic" \
            --if-not-exists 2>/dev/null || true
    done

    log_info "Kafka topics created!"
}

# Setup MinIO buckets
setup_minio() {
    print_section "Setting Up MinIO"

    log_step "Waiting for MinIO to be ready..."
    sleep 10

    # Create buckets using MinIO client
    docker-compose -f "$COMPOSE_FILE" exec -T minio \
        mc alias set local http://localhost:9000 minioadmin minioadmin123 2>/dev/null || true

    local buckets=("ecommerce-data-lake" "raw-data" "processed-data" "analytics")

    for bucket in "${buckets[@]}"; do
        log_step "Creating bucket: $bucket"
        docker-compose -f "$COMPOSE_FILE" exec -T minio \
            mc mb local/$bucket --ignore-existing 2>/dev/null || true
    done

    log_info "MinIO buckets created!"
}

# Show service URLs
show_urls() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}   Service URLs${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo -e "  ${GREEN}Airflow UI:${NC}        http://localhost:8081"
    echo -e "  ${GREEN}Spark Master UI:${NC}   http://localhost:8080"
    echo -e "  ${GREEN}Grafana:${NC}           http://localhost:3000"
    echo -e "  ${GREEN}Metabase:${NC}          http://localhost:3001"
    echo -e "  ${GREEN}MinIO Console:${NC}     http://localhost:9001"
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}   Database Connections${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo -e "  ${GREEN}Source DB:${NC}     localhost:5432/ecommerce_source"
    echo -e "  ${GREEN}Warehouse DB:${NC}  localhost:5433/ecommerce_warehouse"
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}   Default Credentials${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo -e "  ${GREEN}Airflow:${NC}       admin / admin"
    echo -e "  ${GREEN}Grafana:${NC}       admin / admin"
    echo -e "  ${GREEN}Metabase:${NC}      (setup on first access)"
    echo -e "  ${GREEN}MinIO:${NC}         minioadmin / minioadmin123"
    echo -e "  ${GREEN}Source DB:${NC}     ecommerce_user / ecommerce_pass"
    echo -e "  ${GREEN}Warehouse DB:${NC}  warehouse_user / warehouse_pass"
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}   Kafka${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo -e "  ${GREEN}Bootstrap:${NC}     localhost:9092"
    echo -e "  ${GREEN}Zookeeper:${NC}     localhost:2181"
    echo ""
}

# Show help
show_help() {
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start       Start all services (default)"
    echo "  stop        Stop all services"
    echo "  restart     Restart all services"
    echo "  status      Show status of all services"
    echo "  logs        Show logs (follow mode)"
    echo "  clean       Stop and remove all containers, volumes, and networks"
    echo "  init        Initialize databases and sample data only"
    echo "  urls        Show service URLs and credentials"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0              # Start all services"
    echo "  $0 start        # Start all services"
    echo "  $0 stop         # Stop all services"
    echo "  $0 logs kafka   # Show Kafka logs"
    echo ""
}

# Show status
show_status() {
    print_section "Service Status"
    docker-compose -f "$COMPOSE_FILE" ps
}

# Stop services
stop_services() {
    print_section "Stopping Services"
    docker-compose -f "$COMPOSE_FILE" down
    log_info "All services stopped!"
}

# Clean up everything
clean_all() {
    print_section "Cleaning Up"

    log_warn "This will remove all containers, volumes, and networks!"
    read -p "Are you sure? (y/N): " confirm

    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans
        docker system prune -f
        log_info "Cleanup complete!"
    else
        log_info "Cleanup cancelled"
    fi
}

# Show logs
show_logs() {
    local service=$1
    if [ -n "$service" ]; then
        docker-compose -f "$COMPOSE_FILE" logs -f "$service"
    else
        docker-compose -f "$COMPOSE_FILE" logs -f
    fi
}

# Health check
health_check() {
    print_section "Health Check"

    local services=("postgres-source" "postgres-warehouse" "kafka" "zookeeper" "minio" "spark-master")
    local all_healthy=true

    for service in "${services[@]}"; do
        if docker-compose -f "$COMPOSE_FILE" ps | grep -q "$service.*Up"; then
            echo -e "  ${GREEN}✓${NC} $service is running"
        else
            echo -e "  ${RED}✗${NC} $service is not running"
            all_healthy=false
        fi
    done

    echo ""

    if $all_healthy; then
        log_info "All services are healthy!"
        return 0
    else
        log_error "Some services are not running"
        return 1
    fi
}

# Run complete setup
run_setup() {
    check_prerequisites
    setup_environment
    create_sample_data
    pull_images
    start_services
    init_databases
    create_kafka_topics
    setup_minio
    health_check
    show_urls

    print_section "Setup Complete!"
    echo -e "${GREEN}The E-Commerce Data Pipeline Platform is now running!${NC}"
    echo ""
    echo -e "To view logs: ${CYAN}$0 logs${NC}"
    echo -e "To stop:      ${CYAN}$0 stop${NC}"
    echo -e "To restart:   ${CYAN}$0 restart${NC}"
    echo ""
}

# Main function
main() {
    print_banner

    local command=${1:-start}

    case $command in
        start)
            run_setup
            ;;
        stop)
            stop_services
            ;;
        restart)
            stop_services
            run_setup
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs "$2"
            ;;
        clean)
            clean_all
            ;;
        init)
            setup_environment
            create_sample_data
            init_databases
            create_kafka_topics
            setup_minio
            ;;
        urls)
            show_urls
            ;;
        health)
            health_check
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            show_help
            exit 1
            ;;
    esac
}

# Run main
main "$@"
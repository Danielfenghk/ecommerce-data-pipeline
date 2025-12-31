#!/bin/bash

# =============================================================================
# E-Commerce Data Pipeline - Enhanced Start Script
# Compatible with Windows (PowerShell) and Unix-like environments
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

# =============================================================================
# STEP 1: Check Prerequisites
# =============================================================================
check_prerequisites() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 1: Checking Prerequisites${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker not found. Please install Docker."
        exit 1
    fi
    log_info "Docker: $(docker --version)"
    
    if ! docker info &> /dev/null; then
        log_error "Docker daemon not running. Please start Docker."
        exit 1
    fi
    log_info "Docker daemon is running"
    
    log_info "✅ Prerequisites check passed"
}

# =============================================================================
# STEP 2: Setup Environment
# =============================================================================
setup_environment() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 2: Setting Up Environment${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Create directories
    mkdir -p "${PROJECT_ROOT}/data/sample"
    mkdir -p "${PROJECT_ROOT}/data/raw"
    mkdir -p "${PROJECT_ROOT}/data/processed"
    mkdir -p "${PROJECT_ROOT}/logs"
    mkdir -p "${PROJECT_ROOT}/dags"
    mkdir -p "${PROJECT_ROOT}/sql"
    
    log_info "Created project directories"
    
    # Create .env if not exists
    if [ ! -f "${PROJECT_ROOT}/.env" ]; then
        cp "${PROJECT_ROOT}/.env.example" "${PROJECT_ROOT}/.env" 2>/dev/null || create_env_file
        log_info "Created .env file"
    fi
    
    log_info "✅ Environment setup complete"
}

create_env_file() {
    cat > "${PROJECT_ROOT}/.env" << 'EOF'
SOURCE_DB_HOST=postgres-source
SOURCE_DB_PORT=5432
SOURCE_DB_NAME=ecommerce_source
SOURCE_DB_USER=ecommerce_user
SOURCE_DB_PASSWORD=ecommerce_pass
WAREHOUSE_DB_HOST=postgres-warehouse
WAREHOUSE_DB_PORT=5432
WAREHOUSE_DB_NAME=ecommerce_warehouse
WAREHOUSE_DB_USER=warehouse_user
WAREHOUSE_DB_PASSWORD=warehouse_pass
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
EOF
}

# =============================================================================
# STEP 3: Create SQL Initialization Files
# =============================================================================
create_sql_files() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 3: Creating SQL Initialization Files${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Create source tables SQL
    cat > "${PROJECT_ROOT}/sql/init_source.sql" << 'EOSQL'
-- Source Database Initialization
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    registration_date TIMESTAMP,
    customer_segment VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    order_date TIMESTAMP,
    status VARCHAR(50),
    payment_method VARCHAR(50),
    subtotal DECIMAL(12,2),
    tax DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2) DEFAULT 0,
    total_price DECIMAL(12,2)
);

-- Insert sample customers
INSERT INTO customers (customer_id, first_name, last_name, email, city, state, country, postal_code, registration_date, customer_segment) VALUES
('CUST001', 'John', 'Smith', 'john.smith@email.com', 'New York', 'NY', 'USA', '10001', '2023-06-15', 'Premium'),
('CUST002', 'Emily', 'Johnson', 'emily.j@email.com', 'Los Angeles', 'CA', 'USA', '90001', '2023-07-20', 'Regular'),
('CUST003', 'Michael', 'Williams', 'm.williams@email.com', 'Chicago', 'IL', 'USA', '60601', '2023-08-10', 'Premium'),
('CUST004', 'Sarah', 'Brown', 'sarah.brown@email.com', 'Houston', 'TX', 'USA', '77001', '2023-09-05', 'Regular'),
('CUST005', 'David', 'Jones', 'david.jones@email.com', 'Phoenix', 'AZ', 'USA', '85001', '2023-10-12', 'New')
ON CONFLICT (customer_id) DO NOTHING;

-- Insert sample products
INSERT INTO products (product_id, name, category, subcategory, brand, price, cost, stock_quantity) VALUES
('PROD001', 'Wireless Bluetooth Headphones', 'Electronics', 'Audio', 'SoundMax', 79.99, 45.00, 150),
('PROD002', 'Organic Green Tea', 'Food & Beverages', 'Tea', 'NatureBrew', 12.99, 6.50, 500),
('PROD003', 'Running Shoes Pro', 'Sports & Outdoors', 'Footwear', 'SpeedRunner', 129.99, 75.00, 200),
('PROD004', 'Stainless Steel Water Bottle', 'Home & Kitchen', 'Drinkware', 'EcoLife', 24.99, 12.00, 300),
('PROD005', 'Smart Watch Series 5', 'Electronics', 'Wearables', 'TechTime', 299.99, 180.00, 100)
ON CONFLICT (product_id) DO NOTHING;

-- Insert sample orders
INSERT INTO orders (order_id, customer_id, order_date, status, payment_method, subtotal, tax, shipping_cost, total_amount) VALUES
('ORD-001', 'CUST001', '2024-01-15 10:30:00', 'completed', 'credit_card', 129.97, 10.40, 5.99, 146.36),
('ORD-002', 'CUST003', '2024-01-16 14:45:00', 'completed', 'paypal', 269.99, 21.60, 0, 291.59),
('ORD-003', 'CUST002', '2024-01-17 09:15:00', 'shipped', 'credit_card', 169.98, 13.60, 7.99, 191.57),
('ORD-004', 'CUST005', '2024-01-18 16:00:00', 'processing', 'debit_card', 76.95, 6.16, 5.99, 89.10),
('ORD-005', 'CUST004', '2024-01-19 11:30:00', 'completed', 'credit_card', 109.97, 8.80, 0, 118.77)
ON CONFLICT (order_id) DO NOTHING;

-- Insert sample order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount, total_price) VALUES
('ORD-001', 'PROD001', 1, 79.99, 0, 79.99),
('ORD-001', 'PROD004', 2, 24.99, 0, 49.98),
('ORD-002', 'PROD005', 1, 299.99, 30.00, 269.99),
('ORD-003', 'PROD003', 1, 129.99, 0, 129.99),
('ORD-003', 'PROD004', 1, 24.99, 0, 24.99),
('ORD-004', 'PROD002', 3, 12.99, 0, 38.97),
('ORD-004', 'PROD004', 1, 24.99, 0, 24.99),
('ORD-005', 'PROD001', 1, 79.99, 0, 79.99),
('ORD-005', 'PROD002', 2, 12.99, 0, 25.98)
ON CONFLICT DO NOTHING;
EOSQL

    log_info "Created sql/init_source.sql"

    # Create warehouse tables SQL
    cat > "${PROJECT_ROOT}/sql/init_warehouse.sql" << 'EOSQL'
-- Warehouse Database Initialization

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    day_of_month INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    registration_date DATE,
    customer_tenure_days INTEGER,
    is_current BOOLEAN DEFAULT TRUE,
    effective_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    current_price DECIMAL(10,2),
    cost DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    price_tier VARCHAR(50),
    is_current BOOLEAN DEFAULT TRUE,
    effective_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    subtotal DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_amount DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    item_count INTEGER,
    order_value_tier VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Sales Fact
CREATE TABLE IF NOT EXISTS fact_daily_sales (
    daily_sales_key SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    total_orders INTEGER,
    total_revenue DECIMAL(14,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL Job Log
CREATE TABLE IF NOT EXISTS etl_job_log (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(200),
    status VARCHAR(50),
    records_processed INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Results
CREATE TABLE IF NOT EXISTS data_quality_results (
    result_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_name VARCHAR(200),
    passed BOOLEAN,
    details JSONB,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate Date Dimension (2023-2025)
INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month, 
                      week_of_year, month_number, month_name, quarter, year, is_weekend)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(DOW FROM d)::INTEGER,
    TO_CHAR(d, 'Day'),
    EXTRACT(DAY FROM d)::INTEGER,
    EXTRACT(WEEK FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2023-01-01'::date, '2025-12-31'::date, '1 day'::interval) d
ON CONFLICT (date_key) DO NOTHING;
EOSQL

    log_info "Created sql/init_warehouse.sql"
    log_info "✅ SQL files created"
}

# =============================================================================
# STEP 4: Create Sample Data Files
# =============================================================================
create_sample_data() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 4: Creating Sample Data Files${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Create products.csv
    cat > "${PROJECT_ROOT}/data/sample/products.csv" << 'EOF'
product_id,name,category,brand,price,cost,stock_quantity
PROD001,Wireless Bluetooth Headphones,Electronics,SoundMax,79.99,45.00,150
PROD002,Organic Green Tea,Food & Beverages,NatureBrew,12.99,6.50,500
PROD003,Running Shoes Pro,Sports & Outdoors,SpeedRunner,129.99,75.00,200
PROD004,Stainless Steel Water Bottle,Home & Kitchen,EcoLife,24.99,12.00,300
PROD005,Smart Watch Series 5,Electronics,TechTime,299.99,180.00,100
EOF
    log_info "Created data/sample/products.csv"
    
    # Create customers.csv
    cat > "${PROJECT_ROOT}/data/sample/customers.csv" << 'EOF'
customer_id,first_name,last_name,email,city,state,country,customer_segment
CUST001,John,Smith,john.smith@email.com,New York,NY,USA,Premium
CUST002,Emily,Johnson,emily.j@email.com,Los Angeles,CA,USA,Regular
CUST003,Michael,Williams,m.williams@email.com,Chicago,IL,USA,Premium
CUST004,Sarah,Brown,sarah.brown@email.com,Houston,TX,USA,Regular
CUST005,David,Jones,david.jones@email.com,Phoenix,AZ,USA,New
EOF
    log_info "Created data/sample/customers.csv"
    
    # Create orders.json
    cat > "${PROJECT_ROOT}/data/sample/orders.json" << 'EOF'
[
    {"order_id": "ORD-001", "customer_id": "CUST001", "order_date": "2024-01-15T10:30:00Z", "status": "completed", "total_amount": 146.36},
    {"order_id": "ORD-002", "customer_id": "CUST003", "order_date": "2024-01-16T14:45:00Z", "status": "completed", "total_amount": 291.59},
    {"order_id": "ORD-003", "customer_id": "CUST002", "order_date": "2024-01-17T09:15:00Z", "status": "shipped", "total_amount": 191.57},
    {"order_id": "ORD-004", "customer_id": "CUST005", "order_date": "2024-01-18T16:00:00Z", "status": "processing", "total_amount": 89.10},
    {"order_id": "ORD-005", "customer_id": "CUST004", "order_date": "2024-01-19T11:30:00Z", "status": "completed", "total_amount": 118.77}
]
EOF
    log_info "Created data/sample/orders.json"
    
    log_info "✅ Sample data files created"
}

# =============================================================================
# STEP 5: Start Docker Services
# =============================================================================
start_services() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 5: Starting Docker Services${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    log_step "Pulling Docker images (this may take a few minutes)..."
    docker-compose -f "$COMPOSE_FILE" pull 2>&1 | tail -5
    
    log_step "Starting containers..."
    docker-compose -f "$COMPOSE_FILE" up -d
    
    log_info "Waiting for services to initialize (30 seconds)..."
    sleep 30
    
    log_info "✅ Docker services started"
}

# =============================================================================
# STEP 6: Initialize Databases
# =============================================================================
init_databases() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 6: Initializing Databases${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    log_step "Waiting for PostgreSQL to be ready..."
    sleep 10
    
    # Initialize source database
    log_step "Initializing source database..."
    if command -v powershell.exe >/dev/null 2>&1; then
        # Windows/PowerShell environment
        powershell.exe -Command "Get-Content '${PROJECT_ROOT}/sql/init_source.sql' | docker exec -i postgres-source psql -U ecommerce_user -d ecommerce_source" 2>/dev/null || {
            log_warn "Source DB init via file failed, trying inline..."
            docker exec postgres-source psql -U ecommerce_user -d ecommerce_source -c "SELECT 1" >/dev/null 2>&1
        }
    else
        # Unix-like environment
        docker exec -i postgres-source psql -U ecommerce_user -d ecommerce_source < "${PROJECT_ROOT}/sql/init_source.sql" 2>/dev/null || {
            log_warn "Source DB init via file failed, trying inline..."
            docker exec postgres-source psql -U ecommerce_user -d ecommerce_source -c "SELECT 1" >/dev/null 2>&1
        }
    fi
    
    # Verify source data
    CUSTOMER_COUNT=$(docker exec postgres-source psql -U ecommerce_user -d ecommerce_source -t -c "SELECT COUNT(*) FROM customers" 2>/dev/null | tr -d ' ')
    log_info "Source DB - Customers: ${CUSTOMER_COUNT:-0}"
    
    # Initialize warehouse database
    log_step "Initializing warehouse database..."
    if command -v powershell.exe >/dev/null 2>&1; then
        # Windows/PowerShell environment
        powershell.exe -Command "Get-Content '${PROJECT_ROOT}/sql/init_warehouse.sql' | docker exec -i postgres-warehouse psql -U warehouse_user -d ecommerce_warehouse" 2>/dev/null || {
            log_warn "Warehouse DB init via file failed, trying inline..."
        }
    else
        # Unix-like environment
        docker exec -i postgres-warehouse psql -U warehouse_user -d ecommerce_warehouse < "${PROJECT_ROOT}/sql/init_warehouse.sql" 2>/dev/null || {
            log_warn "Warehouse DB init via file failed, trying inline..."
        }
    fi
    
    # Verify warehouse
    DATE_COUNT=$(docker exec postgres-warehouse psql -U warehouse_user -d ecommerce_warehouse -t -c "SELECT COUNT(*) FROM dim_date" 2>/dev/null | tr -d ' ')
    log_info "Warehouse DB - Date dimension rows: ${DATE_COUNT:-0}"
    
    log_info "✅ Databases initialized"
}

# =============================================================================
# STEP 7: Create Kafka Topics
# =============================================================================
create_kafka_topics() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 7: Creating Kafka Topics${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    log_step "Waiting for Kafka to be ready..."
    sleep 10
    
    topics=("orders" "products" "customers" "events" "processed-data" "alerts")
    
    for topic in "${topics[@]}"; do
        docker exec kafka kafka-topics --create \
            --bootstrap-server localhost:9092 \
            --replication-factor 1 \
            --partitions 3 \
            --topic "$topic" \
            --if-not-exists 2>/dev/null && log_info "Created topic: $topic" || log_warn "Topic $topic may already exist"
    done
    
    log_info "✅ Kafka topics created"
}

# =============================================================================
# STEP 8: Setup MinIO Buckets
# =============================================================================
setup_minio() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 8: Setting Up MinIO Data Lake${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    log_step "Configuring MinIO client..."
    docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin123 >/dev/null 2>&1 && \
        log_info "MinIO client configured" || \
        log_warn "MinIO client configuration may have issues"

    log_step "Creating MinIO buckets..."

    buckets=("raw-data" "processed-data" "analytics" "checkpoints")

    for bucket in "${buckets[@]}"; do
        # Create bucket using MinIO client inside container
        docker exec minio mc mb local/$bucket --ignore-existing >/dev/null 2>&1 && \
            log_info "Created bucket: $bucket" || \
            log_warn "Bucket $bucket may already exist"
    done

    log_info "✅ MinIO buckets configured"
}

# =============================================================================
# STEP 9: Health Check
# =============================================================================
health_check() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  Step 9: Health Check${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    services=("postgres-source" "postgres-warehouse" "kafka" "zookeeper" "minio" "spark-master" "redis")
    
    all_healthy=true
    for service in "${services[@]}"; do
        if docker-compose -f "$COMPOSE_FILE" ps | grep -q "$service.*Up"; then
            echo -e "  ${GREEN}✓${NC} $service is running"
        else
            echo -e "  ${RED}✗${NC} $service is NOT running"
            all_healthy=false
        fi
    done
    
    # Check optional services
    optional_services=("airflow-webserver" "grafana" "metabase")
    for service in "${optional_services[@]}"; do
        if docker-compose -f "$COMPOSE_FILE" ps | grep -q "$service.*Up"; then
            echo -e "  ${GREEN}✓${NC} $service is running"
        else
            echo -e "  ${YELLOW}○${NC} $service is starting..."
        fi
    done
    
    if $all_healthy; then
        log_info "✅ All core services are healthy"
    else
        log_warn "Some services may still be starting"
    fi
}

# =============================================================================
# STEP 10: Show Service URLs
# =============================================================================
show_urls() {
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                    🎉 Setup Complete!                         ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${GREEN}Service URLs:${NC}"
    echo -e "  • Airflow UI:        ${CYAN}http://localhost:8081${NC}  (admin/admin)"
    echo -e "  • Spark Master:      ${CYAN}http://localhost:8080${NC}"
    echo -e "  • Grafana:           ${CYAN}http://localhost:3000${NC}  (admin/admin)"
    echo -e "  • Metabase:          ${CYAN}http://localhost:3001${NC}"
    echo -e "  • MinIO Console:     ${CYAN}http://localhost:9001${NC}  (minioadmin/minioadmin123)"
    echo ""
    echo -e "${GREEN}Database Connections:${NC}"
    echo -e "  • Source DB:         localhost:5432  (ecommerce_user/ecommerce_pass)"
    echo -e "  • Warehouse DB:      localhost:5433  (warehouse_user/warehouse_pass)"
    echo ""
    echo -e "${GREEN}Kafka:${NC}"
    echo -e "  • Bootstrap Server:  localhost:9092"
    echo ""
    echo -e "${YELLOW}Next Steps:${NC}"
    echo -e "  1. Run ETL:          python run_etl_manual.py"
    echo -e "  2. Send Events:      python kafka_event_producer.py"
    echo -e "  3. Configure Grafana: Add PostgreSQL data source"
    echo -e "  4. View Logs:        docker-compose logs -f [service]"
    echo ""
}

# =============================================================================
# MAIN
# =============================================================================
main() {
    echo ""
    echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║       E-Commerce Data Pipeline - Automated Setup              ║${NC}"
    echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
    
    check_prerequisites     # Step 1
    setup_environment       # Step 2
    create_sql_files        # Step 3
    create_sample_data      # Step 4
    start_services          # Step 5
    init_databases          # Step 6
    create_kafka_topics     # Step 7
    setup_minio             # Step 8
    health_check            # Step 9
    show_urls               # Step 10
}

# Handle commands
case "${1:-start}" in
    start)
        main
        ;;
    stop)
        docker-compose -f "$COMPOSE_FILE" down
        log_info "All services stopped"
        ;;
    restart)
        docker-compose -f "$COMPOSE_FILE" down
        main
        ;;
    status)
        docker-compose -f "$COMPOSE_FILE" ps
        ;;
    logs)
        docker-compose -f "$COMPOSE_FILE" logs -f ${2:-}
        ;;
    clean)
        docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans
        log_info "Cleaned up all containers and volumes"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|clean}"
        exit 1
        ;;
esac
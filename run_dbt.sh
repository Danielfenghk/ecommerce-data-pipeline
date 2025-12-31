#!/bin/bash

# dbt Data Pipeline Execution Script
# This script runs the complete dbt workflow as per dbt_project/readme.md

echo "🚀 Starting dbt Data Pipeline Execution"
echo "========================================"

# Step 1: Install dbt with PostgreSQL adapter
echo "📦 Step 1: Installing dbt with PostgreSQL adapter..."
pip install dbt-postgres

# Step 2: Verify installation
echo "✅ Step 2: Verifying dbt installation..."
dbt --version

# Step 3: Setup dbt Profile
echo "🔧 Step 3: Setting up dbt profile..."

# Create .dbt directory
mkdir -p ~/.dbt

# Create profiles.yml
cat > ~/.dbt/profiles.yml << 'EOF'
ecommerce:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433
      user: warehouse_user
      password: warehouse_pass
      dbname: ecommerce_warehouse
      schema: public
      threads: 4
EOF

echo "✅ dbt profile created successfully"

# Step 4: Navigate to dbt project and install packages
echo "📁 Step 4: Installing dbt packages..."
cd dbt_project
dbt deps

# Step 5: Test connection
echo "🔍 Step 5: Testing dbt connection..."
dbt debug

# Step 6: Run all models
echo "🏃 Step 6: Running all dbt models..."
dbt run

# Step 7: Run tests
echo "🧪 Step 7: Running dbt tests..."
dbt test

# Step 8: Generate documentation
echo "📚 Step 8: Generating dbt documentation..."
dbt docs generate

# Step 9: Serve documentation
echo "🌐 Step 9: Serving dbt documentation..."
echo "Documentation will be available at http://localhost:8083"
dbt docs serve --port 8083

echo "🎉 dbt Data Pipeline Execution Completed Successfully!"
echo "======================================================"

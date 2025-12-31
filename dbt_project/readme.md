How to Run dbt
Step 1: Install dbt
PowerShell

# Install dbt with PostgreSQL adapter
pip install dbt-postgres

# Verify installation
dbt --version
Step 2: Setup dbt Profile
Create ~/.dbt/profiles.yml (in your user home directory):

PowerShell

# Create .dbt directory
mkdir ~/.dbt -Force

# Create profiles.yml
@"
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
"@ | Out-File -FilePath "$env:USERPROFILE\.dbt\profiles.yml" -Encoding utf8
Step 3: Initialize and Run dbt
PowerShell

# Navigate to dbt project
cd dbt_project

# Install packages
dbt deps

# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate

# Serve documentation (opens browser)
dbt docs serve
#!/bin/bash
# This script runs automatically when PostgreSQL container starts for the first time

# Exit immediately if any command fails
set -e

echo "=== PostgreSQL Initialization Script Starting ==="

# These environment variables come from docker-compose.yml:
# $POSTGRES_USER = admin (from your .env file)
# $POSTGRES_DB = metastore_db (from your .env file)

echo "Creating Airflow database for user: $POSTGRES_USER"

# Run SQL commands using psql
# -v ON_ERROR_STOP=1: Stop if any SQL command fails
# --username "$POSTGRES_USER": Connect as admin user
# --dbname "$POSTGRES_DB": Connect to metastore_db database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create a new database called 'airflow'
    CREATE DATABASE airflow;
    
    -- Give the admin user full access to the airflow database
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
    
    -- Allow admin user to create databases (Airflow needs this permission)
    ALTER USER $POSTGRES_USER CREATEDB;
    
    -- Show all databases to confirm airflow was created
    \l
EOSQL

echo "=== Airflow database created successfully! ==="
echo "Available databases:"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "\l"
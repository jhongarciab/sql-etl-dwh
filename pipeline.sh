#!/bin/bash

# Pipeline Script: Full Data Warehouse Pipeline
# Script Purpose:
#     This script orchestrates the full ETL pipeline for the Data Warehouse.
#     It performs the following actions:
#     - Drops and recreates the database for full reproducibility.
#     - Initializes schemas (bronze, silver, gold).
#     - Loads raw data into the Bronze layer.
#     - Transforms and cleanses data into the Silver layer.
#     - Creates analytical views in the Gold layer.
#     - Runs quality checks on Silver and Gold layers.
#
# Usage:
#     ./pipeline.sh -U <postgres_user>
#
# Parameters:
#     -U  PostgreSQL user to connect with.
#     None. Database name is fixed as 'DataWarehouse'.

set -e

# Parse arguments
while getopts "U:" opt; do
    case $opt in
        U) PG_USER="$OPTARG" ;;
        *) echo "Usage: $0 -U <postgres_user>"; exit 1 ;;
    esac
done

if [ -z "$PG_USER" ]; then
    echo "Error: PostgreSQL user is required. Use -U <user>"
    exit 1
fi

read -s -p "Password for user $PG_USER: " PGPASSWORD
echo ""
export PGPASSWORD

DB_NAME="DataWarehouse"
SCRIPTS_DIR="$(dirname "$0")/scripts"
TEST_DIR="$(dirname "$0")/test"
PIPELINE_START=$SECONDS
LOG_DIR="$(dirname "$0")/docs/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/pipeline_$(date +%Y%m%d_%H%M%S).log"

main() {

# Helper: run a SQL file and report execution time
run_sql() {
    local description="$1"
    local database="$2"
    local file="$3"
    local step_start=$SECONDS

    echo ""
    echo ">> $description"
    psql -U "$PG_USER" -d "$database" -f "$file"
    echo "   Done in $((SECONDS - step_start))s"
}

echo "============================================="
echo " Data Warehouse Pipeline"
echo " User:     $PG_USER"
echo " Database: $DB_NAME"
echo "============================================="

# Drop and recreate the database
echo ""
echo ">> Terminating active connections to '$DB_NAME'..."
psql -U "$PG_USER" -d postgres -c "
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();
"

echo ">> Dropping and recreating database '$DB_NAME'..."
step_start=$SECONDS
psql -U "$PG_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";"
psql -U "$PG_USER" -d postgres -c "CREATE DATABASE \"$DB_NAME\";"
echo "   Done in $((SECONDS - step_start))s"

# Initialize schemas
run_sql "Creating schemas (bronze, silver, gold)" "$DB_NAME" "$SCRIPTS_DIR/db/init_db.sql"

# Bronze layer
run_sql "Creating Bronze tables"    "$DB_NAME" "$SCRIPTS_DIR/db/bronze/ddl_bronze.sql"
run_sql "Creating Bronze procedure" "$DB_NAME" "$SCRIPTS_DIR/db/bronze/proc_bronze.sql"

echo ""
step_start=$SECONDS
echo ">> Truncating Bronze tables..."
psql -U "$PG_USER" -d "$DB_NAME" -c "CALL bronze.load_bronze();"
echo "   Done in $((SECONDS - step_start))s"

run_sql "Loading raw data into Bronze" "$DB_NAME" "$SCRIPTS_DIR/db/bronze/load_bronze.sql"

# Silver layer
run_sql "Creating Silver tables"    "$DB_NAME" "$SCRIPTS_DIR/db/silver/ddl_silver.sql"
run_sql "Creating Silver procedure" "$DB_NAME" "$SCRIPTS_DIR/db/silver/proc_silver.sql"

echo ""
step_start=$SECONDS
echo ">> Running Silver ETL (Bronze -> Silver)..."
psql -U "$PG_USER" -d "$DB_NAME" -c "CALL silver.load_silver();"
echo "   Done in $((SECONDS - step_start))s"

# Silver quality checks
run_sql "Creating quality check procedures" "$DB_NAME" "$TEST_DIR/proc_quality.sql"

echo ""
step_start=$SECONDS
echo ">> Running Silver quality checks..."
psql -U "$PG_USER" -d "$DB_NAME" -c "CALL silver.quality_check();"
echo "   Done in $((SECONDS - step_start))s"

# Gold layer
run_sql "Creating Gold views (dim_customers, dim_products, fact_sales)" "$DB_NAME" "$SCRIPTS_DIR/db/gold/load_gold.sql"

# Gold quality checks
echo ""
step_start=$SECONDS
echo ">> Running Gold quality checks..."
psql -U "$PG_USER" -d "$DB_NAME" -c "CALL gold.quality_check();"
echo "   Done in $((SECONDS - step_start))s"

echo ""
echo "============================================="
echo " Pipeline completed successfully"
echo " Total time: $((SECONDS - PIPELINE_START))s"
echo "============================================="

}

main 2>&1 | tee "$LOG_FILE"
#!/bin/bash

# =============================================================================
# pipeline.sh — Full Data Warehouse Pipeline
# Usage: ./pipeline.sh -U <postgres_user>
# =============================================================================

set -e  # Exit immediately if any command fails

# --- Parse arguments ---------------------------------------------------------
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

# --- Helper function ---------------------------------------------------------
run_sql() {
    local description="$1"
    local database="$2"
    local file="$3"

    echo ""
    echo ">> $description"
    psql -U "$PG_USER" -d "$database" -f "$file"
}

# =============================================================================
echo "============================================="
echo " Data Warehouse Pipeline"
echo " User: $PG_USER"
echo " Database: $DB_NAME"
echo "============================================="

# --- Step 1: Drop and recreate the database ----------------------------------
echo ""
echo ">> Terminating active connections to '$DB_NAME'..."
psql -U "$PG_USER" -d postgres -c "
    SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();
"

echo ">> Dropping and recreating database '$DB_NAME'..."
psql -U "$PG_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";"
psql -U "$PG_USER" -d postgres -c "CREATE DATABASE \"$DB_NAME\";"

# --- Step 2: Initialize schemas ----------------------------------------------
run_sql "Creating schemas (bronze, silver, gold)" "$DB_NAME" "$SCRIPTS_DIR/db/init_db.sql"

# --- Step 3: Bronze layer ----------------------------------------------------
run_sql "Creating Bronze tables"     "$DB_NAME" "$SCRIPTS_DIR/db/bronze/ddl_bronze.sql"
run_sql "Creating Bronze procedure"  "$DB_NAME" "$SCRIPTS_DIR/db/bronze/proc_bronze.sql"

echo ""
echo ">> Truncating Bronze tables..."
psql -U "$PG_USER" -d "$DB_NAME" -c "CALL bronze.load_bronze();"

run_sql "Loading raw data into Bronze" "$DB_NAME" "$SCRIPTS_DIR/db/bronze/load_bronze.sql"

# --- Step 4: Silver layer ----------------------------------------------------
run_sql "Creating Silver tables"     "$DB_NAME" "$SCRIPTS_DIR/db/silver/ddl_silver.sql"
run_sql "Creating Silver procedure"  "$DB_NAME" "$SCRIPTS_DIR/db/silver/proc_silver.sql"

echo ""
echo ">> Running Silver ETL (Bronze -> Silver)..."
psql -U "$PG_USER" -d "$DB_NAME" -c "CALL silver.load_silver();"

# --- Step 5: Gold layer ------------------------------------------------------
run_sql "Creating Gold views (dim_customers, dim_products, fact_sales)" "$DB_NAME" "$SCRIPTS_DIR/db/gold/load_gold.sql"

# =============================================================================
echo ""
echo "============================================="
echo " Pipeline completed successfully"
echo "============================================="

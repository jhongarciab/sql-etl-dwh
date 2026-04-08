/*
Load Script: Bronze Layer
Script Purpose:
    Loads raw CSV data into the 'bronze' schema from external CSV files.
    Uses psql \COPY, so it must be executed via psql:

        psql -U <user> -d <database> -f scripts/bronze/load_bronze.sql

    Run from the project root so relative paths resolve correctly.

Note:
    Truncation is handled separately by CALL bronze.load_bronze()
    before running this script.

Parameters:
    None.
*/

\set ON_ERROR_STOP on

\echo 'CRM - customers...'
\COPY bronze.crm_cust_info FROM './datasets/source_crm/cust_info.csv' CSV HEADER DELIMITER ',';

\echo 'CRM - products...'
\COPY bronze.crm_prd_info FROM './datasets/source_crm/prd_info.csv' CSV HEADER DELIMITER ',';

\echo 'CRM - sales...'
\COPY bronze.crm_sales_details FROM './datasets/source_crm/sales_details.csv' CSV HEADER DELIMITER ',';

\echo 'ERP - customers...'
\COPY bronze.erp_cust_az12 FROM './datasets/source_erp/CUST_AZ12.csv' CSV HEADER DELIMITER ',';

\echo 'ERP - locations...'
\COPY bronze.erp_loc_a101 FROM './datasets/source_erp/LOC_A101.csv' CSV HEADER DELIMITER ',';

\echo 'ERP - categories...'
\COPY bronze.erp_px_cat_g1v2 FROM './datasets/source_erp/PX_CAT_G1V2.csv' CSV HEADER DELIMITER ',';

\echo 'Bronze table upload completed successfully'

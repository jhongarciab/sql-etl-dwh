/*
Load Data into Bronze Layer
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
*/

\set ON_ERROR_STOP on
\echo 'Cargando CRM - customers...'
\COPY bronze.crm_cust_info FROM '/Users/jhongarciabarrera/VS - Local/Github/sql-data-warehouse/datasets/source_crm/cust_info.csv' CSV HEADER DELIMITER ',';
\echo 'Cargando CRM - products...'
\COPY bronze.crm_prd_info FROM '/Users/jhongarciabarrera/VS - Local/Github/sql-data-warehouse/datasets/source_crm/prd_info.csv' CSV HEADER DELIMITER ',';
\echo 'Cargando CRM - sales...'
\COPY bronze.crm_sales_details FROM '/Users/jhongarciabarrera/VS - Local/Github/sql-data-warehouse/datasets/source_crm/sales_details.csv' CSV HEADER DELIMITER ',';
\echo 'Cargando ERP - customers...'
\COPY bronze.erp_cust_az12 FROM '/Users/jhongarciabarrera/VS - Local/Github/sql-data-warehouse/datasets/source_erp/CUST_AZ12.csv' CSV HEADER DELIMITER ',';
\echo 'Cargando ERP - locations...'
\COPY bronze.erp_loc_a101 FROM '/Users/jhongarciabarrera/VS - Local/Github/sql-data-warehouse/datasets/source_erp/LOC_A101.csv' CSV HEADER DELIMITER ',';
\echo 'Cargando ERP - categories...'
\COPY bronze.erp_px_cat_g1v2 FROM '/Users/jhongarciabarrera/VS - Local/Github/sql-data-warehouse/datasets/source_erp/PX_CAT_G1V2.csv' CSV HEADER DELIMITER ',';
\echo 'Carga BRONZE finalizada correctamente'

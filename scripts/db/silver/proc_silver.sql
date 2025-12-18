/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Cleansing and transforming data from bronze.crm_cust_info to silver.crm_cust_info
    RAISE NOTICE 'Cleansing and transforming data from bronze.crm_cust_info to silver.crm_cust_info';
    RAISE NOTICE 'Truncating silver.crm_cust_info table before inserting cleansed data';

    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE 'Inserting cleansed data into silver.crm_cust_info table';

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_data
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            ELSE 'NaN'
        END,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'NaN'
        END,
        cst_create_data
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cst_id
                   ORDER BY cst_create_data DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    RAISE NOTICE 'Cleansed data inserted into silver.crm_cust_info table successfully';

    -- Cleansing and transforming data from bronze.crm_prd_info to silver.crm_prd_info
    RAISE NOTICE 'Cleansing and transforming data from bronze.crm_prd_info to silver.crm_prd_info';
    RAISE NOTICE 'Truncating silver.crm_prd_info table before inserting cleansed data';

    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE 'Inserting cleansed data into silver.crm_prd_info table';

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7, LENGTH(prd_key)),
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'NaN'
        END,
        prd_start_dt,
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key ORDER BY prd_start_dt
        ) - 1
    FROM bronze.crm_prd_info;

    RAISE NOTICE 'Cleansed data inserted into silver.crm_prd_info table successfully';

    -- Cleansing and transforming data from bronze.crm_sales_details to silver.crm_sales_details
    RAISE NOTICE 'Cleansing and transforming data from bronze.crm_sales_details to silver.crm_sales_details';
    RAISE NOTICE 'Truncating silver.crm_sales_details table before inserting cleansed data';

    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE 'Inserting cleansed data into silver.crm_sales_details table';

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END,
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN COALESCE(sls_sales / NULLIF(sls_quantity, 0), 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    RAISE NOTICE 'Cleansed data inserted into silver.crm_sales_details table successfully';

    -- Cleansing and transforming data from bronze.erp_cust_az12 to silver.erp_cust_az12
    RAISE NOTICE 'Cleansing and transforming data from bronze.erp_cust_az12 to silver.erp_cust_az12';
    RAISE NOTICE 'Truncating silver.erp_cust_az12 table before inserting cleansed data';

    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE 'Inserting cleansed data into silver.erp_cust_az12 table';

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END,
        CASE
            WHEN bdate < DATE '1925-01-01'
              OR bdate > CURRENT_DATE
                THEN NULL
            ELSE bdate
        END,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'NaN'
        END
    FROM bronze.erp_cust_az12;

    RAISE NOTICE 'Cleansed data inserted into silver.erp_cust_az12 table successfully';

    -- Cleansing and transforming data from bronze.erp_loc_a101 to silver.erp_loc_a101
    RAISE NOTICE 'Cleansing and transforming data from bronze.erp_loc_a101 to silver.erp_loc_a101';
    RAISE NOTICE 'Truncating silver.erp_loc_a101 table before inserting cleansed data';

    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE 'Inserting cleansed data into silver.erp_loc_a101 table';

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'NaN'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    RAISE NOTICE 'Cleansed data inserted into silver.erp_loc_a101 table successfully';

    -- Cleansing and transforming data from bronze.erp_px_cat_g1v2 to silver.erp_px_cat_g1v2
    RAISE NOTICE 'Cleansing and transforming data from bronze.erp_px_cat_g1v2 to silver.erp_px_cat_g1v2';
    RAISE NOTICE 'Truncating silver.erp_px_cat_g1v2 table before inserting cleansed data';

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE 'Inserting cleansed data into silver.erp_px_cat_g1v2 table';

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        mainteance
    )
    SELECT
        id,
        cat,
        subcat,
        mainteance
    FROM bronze.erp_px_cat_g1v2;

    RAISE NOTICE 'Cleansed data inserted into silver.erp_px_cat_g1v2 table successfully';

END;
$$;
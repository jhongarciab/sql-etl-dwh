/*
Stored Procedures: Quality Checks
Script Purpose:
    This script creates stored procedures to validate the integrity,
    consistency, and accuracy of the Silver and Gold layers.
    It performs the following actions:
    - Validates Silver tables for nulls, duplicates, and business rules.
    - Validates Gold tables for surrogate key uniqueness and referential integrity.
    - Raises an exception if any check fails, stopping the pipeline.

Parameters:
    None.
    These stored procedures do not accept any parameters or return any values.
*/

-- Silver Quality Checks
CREATE OR REPLACE PROCEDURE silver.quality_check()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INT;
BEGIN
    -- CRM Sources
    -- Customer info - Check for nulls and duplicates in primary key
    SELECT COUNT(*) INTO v_count
    FROM silver.crm_cust_info
    WHERE cst_id IS NULL;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % null values found in silver.crm_cust_info.cst_id', v_count;
    END IF;

    SELECT COUNT(*) INTO v_count
    FROM (
        SELECT cst_id FROM silver.crm_cust_info
        GROUP BY cst_id HAVING COUNT(*) > 1
    ) t;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % duplicate cst_id found in silver.crm_cust_info', v_count;
    END IF;

    -- Customer info - Check for unwanted spaces
    SELECT COUNT(*) INTO v_count
    FROM silver.crm_cust_info
    WHERE TRIM(cst_firstname) != cst_firstname
       OR TRIM(cst_lastname) != cst_lastname
       OR TRIM(cst_marital_status) != cst_marital_status
       OR TRIM(cst_gndr) != cst_gndr;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows with unwanted spaces found in silver.crm_cust_info', v_count;
    END IF;

    -- Product info - Check for nulls and duplicates in primary key
    SELECT COUNT(*) INTO v_count
    FROM silver.crm_prd_info
    WHERE prd_id < 0;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % null values found in silver.crm_prd_info.prd_id', v_count;
    END IF;

    SELECT COUNT(*) INTO v_count
    FROM (
        SELECT prd_id FROM silver.crm_prd_info
        GROUP BY prd_id HAVING COUNT(*) > 1
    ) t;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % duplicate prd_id found in silver.crm_prd_info', v_count;
    END IF;

    -- Product info - Check for nulls or negative costs
    SELECT COUNT(*) INTO v_count
    FROM silver.crm_prd_info
    WHERE prd_cost < 0;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows with negative prd_cost in silver.crm_prd_info', v_count;
    END IF;

    -- Product info - Check for invalid date orders
    SELECT COUNT(*) INTO v_count
    FROM silver.crm_prd_info
    WHERE prd_end_dt < prd_start_dt;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows where prd_end_dt < prd_start_dt in silver.crm_prd_info', v_count;
    END IF;

    -- Sales details - Check for invalid date orders
    SELECT COUNT(*) INTO v_count
    FROM silver.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt
       OR sls_order_dt > sls_due_dt;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows with invalid date order in silver.crm_sales_details', v_count;
    END IF;

    -- Sales details - Business rules validation
    SELECT COUNT(*) INTO v_count
    FROM silver.crm_sales_details
    WHERE sls_sales != sls_quantity * sls_price
       OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
       OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows violating sales business rules in silver.crm_sales_details', v_count;
    END IF;

    -- ERP Sources
    -- ERP customers - Check for out of range birthdates
    SELECT COUNT(*) INTO v_count
    FROM silver.erp_cust_az12
    WHERE bdate < DATE '1925-01-01'
       OR bdate > CURRENT_DATE;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows with out of range bdate in silver.erp_cust_az12', v_count;
    END IF;

    -- ERP categories - Check for unwanted spaces
    SELECT COUNT(*) INTO v_count
    FROM silver.erp_px_cat_g1v2
    WHERE TRIM(cat) != cat
       OR TRIM(subcat) != subcat
       OR TRIM(maintenance) != maintenance;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows with unwanted spaces in silver.erp_px_cat_g1v2', v_count;
    END IF;

    RAISE NOTICE 'All Silver quality checks passed successfully';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Silver quality check error: %', SQLERRM;
END;
$$;


-- Gold Quality Checks
CREATE OR REPLACE PROCEDURE gold.quality_check()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INT;
BEGIN
    -- Check for uniqueness of customer_key in gold.dim_customers
    SELECT COUNT(*) INTO v_count
    FROM (
        SELECT customer_key FROM gold.dim_customers
        GROUP BY customer_key HAVING COUNT(*) > 1
    ) t;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % duplicate customer_key found in gold.dim_customers', v_count;
    END IF;

    -- Check for uniqueness of product_key in gold.dim_products
    SELECT COUNT(*) INTO v_count
    FROM (
        SELECT product_key FROM gold.dim_products
        GROUP BY product_key HAVING COUNT(*) > 1
    ) t;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % duplicate product_key found in gold.dim_products', v_count;
    END IF;

    -- Check referential integrity between fact_sales and dimensions
    SELECT COUNT(*) INTO v_count
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
    LEFT JOIN gold.dim_products p ON p.product_key = f.product_key
    WHERE p.product_key IS NULL OR c.customer_key IS NULL;

    IF v_count > 0 THEN
        RAISE EXCEPTION 'Quality check failed: % rows in gold.fact_sales with missing dimension keys', v_count;
    END IF;

    RAISE NOTICE 'All Gold quality checks passed successfully';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Gold quality check error: %', SQLERRM;
END;
$$;

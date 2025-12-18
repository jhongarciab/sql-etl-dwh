/*
Stored Procedure: Truncate Bronze Layer 
Script Purpose:
    This stored procedure truncates the tables in the 'bronze' schema.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        -- TRY
        TRUNCATE TABLE bronze.crm_cust_info;
        TRUNCATE TABLE bronze.crm_prd_info;
        TRUNCATE TABLE bronze.crm_sales_details;
        TRUNCATE TABLE bronze.erp_cust_az12;
        TRUNCATE TABLE bronze.erp_loc_a101;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE 'Bronze tables truncated successfully';

    EXCEPTION
        WHEN OTHERS THEN
            -- CATCH
            RAISE EXCEPTION
                'Error truncating bronze tables: %',
                SQLERRM;
    END;
END;
$$;

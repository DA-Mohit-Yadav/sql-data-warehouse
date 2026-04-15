/* ============================================================
   PURPOSE
   ============================================================
   This stored procedure loads the SILVER layer from BRONZE
   in the DataWarehouse database.

   It performs:
   - Data cleaning (trimming, standardization)
   - Deduplication (latest records using ROW_NUMBER)
   - Data type conversions (especially date handling)
   - Business rule transformations (gender, marital status, etc.)
   - Basic data validation (invalid dates, null handling)

   The load strategy used is:
   - FULL LOAD (TRUNCATE + INSERT)

   This procedure is part of the ETL pipeline:
   BRONZE (raw) → SILVER (cleaned & standardized)


   ============================================================
   WARNINGS / IMPORTANT NOTES
   ============================================================

   1. FULL REFRESH STRATEGY
      - This procedure TRUNCATES all Silver tables before loading.
      - Do NOT run in production without understanding impact.
      - All existing Silver data will be LOST before reload.

   2. SCHEMA DEPENDENCY
      - INSERT statements depend on exact column order and count.
      - Any schema change in Silver tables may BREAK this procedure.

   3. NO INCREMENTAL LOGIC
      - This is NOT an incremental load.
      - Entire dataset is reprocessed every run.
*/


/* ============================================================
   SILVER LAYER TRANSFORMATION LOGIC
   ============================================================

   GENERAL RULES:
   - Remove duplicates (keep latest records where applicable)
   - Trim all string fields
   - Standardize categorical values
   - Replace invalid/missing values with 'N/A' or NULL
   - Convert data types (especially dates)
   - Ensure data consistency and business logic alignment


   ------------------------------------------------------------
   TABLE: crm_cust_info
   ------------------------------------------------------------
   - Remove duplicate customers using cst_id
       → Keep latest record based on cst_create_date
   - Trim cst_firstname and cst_lastname
   - Standardize marital status:
       M → Married
       S → Single
       Others → 'N/A'
   - Standardize gender:
       M → Male
       F → Female
       Others → 'N/A'
   - Ignore records where cst_id is NULL


   ------------------------------------------------------------
   TABLE: crm_prd_info
   ------------------------------------------------------------
   - Split prd_key into:
       → cat_id (first 5 characters, '-' replaced with '_')
       → prd_key (remaining part)
   - Trim product name (prd_nm)
   - Replace NULL or negative prd_cost with 0
   - Standardize product line:
       M → Mountain
       R → Road
       S → Other Sales
       T → Touring
       Others → 'N/A'
   - Generate prd_end_dt:
       → 1 day before next prd_start_dt per prd_key
       → Use LEAD() window function


   ------------------------------------------------------------
   TABLE: crm_sales_details
   ------------------------------------------------------------
   - Convert date fields from INT (YYYYMMDD) to DATE:
       sls_order_dt, sls_ship_dt, sls_due_dt
   - Use TRY_CONVERT to handle invalid dates safely
   - Calculate sls_sales:
       → If sls_price exists → sls_price * sls_quantity
       → Else → use existing sls_sales
   - Calculate sls_price:
       → If missing → sls_sales / sls_quantity
       → Handle divide-by-zero using NULLIF
   - Round monetary values to 2 decimal places


   ------------------------------------------------------------
   TABLE: erp_cust_az12
   ------------------------------------------------------------
   - Remove 'NAS' prefix from cid
   - Validate birthdate (bdate):
       → Future dates → NULL
       → Dates older than 120 years → NULL
   - Standardize gender:
       → 'M', 'Male' → Male
       → 'F', 'Female' → Female
       → Others → 'N/A'


   ------------------------------------------------------------
   TABLE: erp_loc_a101
   ------------------------------------------------------------
   - Remove '-' from cid
   - Standardize country names:
       DE → Germany
       US / USA → United States
       NULL / empty → 'N/A'
       Others → trimmed value


   ------------------------------------------------------------
   TABLE: erp_px_cat_g1v2
   ------------------------------------------------------------
   - No transformation applied
   - Direct load from Bronze to Silver


   ============================================================
   END OF TRANSFORMATION LOGIC
   ============================================================ */



USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @layer_start DATETIME;
    DECLARE @layer_end DATETIME;

    BEGIN TRY

        PRINT '=====================================';
        PRINT 'Starting Silver Load';
        PRINT 'Start Time: ' + CAST(@start_time AS NVARCHAR);
        PRINT '=====================================';

        --------------------------------------------------
        -- CRM LAYER
        --------------------------------------------------
        SET @layer_start = GETDATE();

        PRINT '-------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------';


        -- crm_cust_info
        PRINT 'Truncating Table :- crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT 'Inserting data :- crm_cust_info';
        INSERT INTO silver.crm_cust_info 
        (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

        SELECT
            cst_id,
            cst_key,
            trim(cst_firstname),
            trim(cst_lastname) ,

            case when upper(trim(cst_marital_status)) = 'M' then 'Married'
                 when upper(trim(cst_marital_status)) = 'S' then 'Single'
                 else 'N/a' 
            end,

            case when upper(trim(cst_gndr)) = 'M' then 'Male'
                 when upper(trim(cst_gndr)) = 'F' then 'Female'
                 else 'N/a' 
            end,

            cst_create_date

        from 
            (select *, row_number() over(partition by cst_id order by cst_create_date desc) as dup_flag
             from bronze.crm_cust_info
             where cst_id is not null ) as dup_flag_tab
        where dup_flag = 1;

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);


        -- crm_prd_info
        PRINT 'Truncating Table :- crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT 'Inserting data :- crm_prd_info';
        INSERT INTO silver.crm_prd_info
        (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

        SELECT
            prd_id,
            replace(substring(prd_key,1,5),'-','_'),
            substring(prd_key,7,len(prd_key)),
            trim(prd_nm),

            CASE 
                WHEN prd_cost < 0 THEN 0
                ELSE ISNULL(prd_cost, 0)
            END,

            case upper(trim(prd_line))
                when 'M'  then 'Mountain'
                when 'R'  then 'Road'
                when 'S'  then 'Other Sales'
                when 'T'  then 'Touring'
                else 'N/a'
            end,

            prd_start_dt,

            DATEADD(DAY, -1, 
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key 
                    ORDER BY prd_start_dt
                )
            )

        from bronze.crm_prd_info;

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);


        -- crm_sales_details
        PRINT 'Truncating Table :- crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT 'Inserting data :- crm_sales_details';
        INSERT INTO silver.crm_sales_details
        (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8)), 112),
            TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8)), 112),
            TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8)), 112),

            CASE 
                WHEN sls_price IS NOT NULL 
                    THEN ABS(sls_price) * sls_quantity
                ELSE sls_sales
            END,

            sls_quantity,

            CASE 
                WHEN sls_price IS NULL 
                    THEN cast(ROUND(sls_sales / NULLIF(sls_quantity, 0), 2) as decimal(12,0))
                ELSE cast(ROUND(ABS(sls_price), 2) as decimal(12,2))
            END

        FROM bronze.crm_sales_details;

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);


        SET @layer_end = GETDATE();
        PRINT 'CRM Layer Time (sec): ' + CAST(DATEDIFF(SECOND, @layer_start, @layer_end) AS NVARCHAR);


        --------------------------------------------------
        -- ERP LAYER
        --------------------------------------------------
        SET @layer_start = GETDATE();

        PRINT '-------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------';


        -- erp_cust_az12
        PRINT 'Truncating Table :- erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT 'Inserting data :- erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)

        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' 
                    THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END,

            CASE 
                WHEN bdate IS NULL THEN NULL
                WHEN bdate > GETDATE() THEN NULL
                WHEN bdate < DATEADD(YEAR, -120, GETDATE()) THEN NULL
                ELSE bdate
            END,

            CASE 
                WHEN LOWER(TRIM(gen)) IN ('m', 'male') THEN 'Male'
                WHEN LOWER(TRIM(gen)) IN ('f', 'female') THEN 'Female'
                ELSE 'n/a'
            END

        FROM bronze.erp_cust_az12;

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);


        -- erp_loc_a101
        PRINT 'Truncating Table :- erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT 'Inserting data :- erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(cid,cntry)

        SELECT
            replace(cid,'-',''),

            case when trim(cntry) = 'DE' then 'Germany'
                 when trim(cntry) in ('US','USA') then 'United States'
                 when trim(cntry) is null or trim(cntry) = '' then 'n/a'
                 else trim(cntry)
            end

        from bronze.erp_loc_a101;

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);


        -- erp_px_cat_g1v2
        PRINT 'Truncating Table :- erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT 'Inserting data :- erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)

        SELECT * FROM bronze.erp_px_cat_g1v2;

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);


        SET @layer_end = GETDATE();
        PRINT 'ERP Layer Time (sec): ' + CAST(DATEDIFF(SECOND, @layer_start, @layer_end) AS NVARCHAR);


        --------------------------------------------------
        -- FINAL
        --------------------------------------------------
        DECLARE @end_time DATETIME = GETDATE();

        PRINT '=====================================';
        PRINT 'Silver Load Completed';
        PRINT 'End Time: ' + CAST(@end_time AS NVARCHAR);
        PRINT 'Total Time (sec): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '=====================================';

    END TRY

    BEGIN CATCH
        PRINT '=====================================';
        PRINT 'Error during Silver Load';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '=====================================';
    END CATCH

END;
GO


-- Execute
EXEC silver.load_silver;

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @layer_start DATETIME;
    DECLARE @layer_end DATETIME;

    BEGIN TRY

        PRINT '=====================================';
        PRINT 'Starting Bronze Load';
        PRINT 'Start Time: ' + CAST(@start_time AS NVARCHAR);
        PRINT '=====================================';

        --------------------------------------------------
        -- CRM TABLES
        --------------------------------------------------
        SET @layer_start = GETDATE();

        PRINT '--- Loading CRM Tables ---';

        ---------------------------
        -- crm_cust_info
        ---------------------------
        PRINT '>>> Truncating: crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>>> Inserting: crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Mohit Yadav\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '-------------------------------------------';

        ---------------------------
        -- crm_prd_info
        ---------------------------
        PRINT '>>> Truncating: crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>>> Inserting: crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Mohit Yadav\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '-------------------------------------------';

        ---------------------------
        -- crm_sales_details
        ---------------------------
        PRINT '>>> Truncating: crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>>> Inserting: crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Mohit Yadav\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '-------------------------------------------';

        SET @layer_end = GETDATE();
        PRINT 'CRM Layer Time (sec): ' + CAST(DATEDIFF(SECOND, @layer_start, @layer_end) AS NVARCHAR);

        --------------------------------------------------
        -- ERP TABLES
        --------------------------------------------------
        SET @layer_start = GETDATE();

        PRINT '--- Loading ERP Tables ---';

        ---------------------------
        -- erp_cust_az12
        ---------------------------
        PRINT '>>> Truncating: erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>>> Inserting: erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Mohit Yadav\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '-------------------------------------------';

        ---------------------------
        -- erp_loc_a101
        ---------------------------
        PRINT '>>> Truncating: erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>>> Inserting: erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Mohit Yadav\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '-------------------------------------------';

        ---------------------------
        -- erp_px_cat_g1v2
        ---------------------------
        PRINT '>>> Truncating: erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>>> Inserting: erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Mohit Yadav\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        PRINT 'Rows Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '-------------------------------------------';

        SET @layer_end = GETDATE();
        PRINT 'ERP Layer Time (sec): ' + CAST(DATEDIFF(SECOND, @layer_start, @layer_end) AS NVARCHAR);

        --------------------------------------------------
        -- TOTAL TIME
        --------------------------------------------------
        DECLARE @end_time DATETIME = GETDATE();

        PRINT '=====================================';
        PRINT 'Bronze Load Completed';
        PRINT 'End Time: ' + CAST(@end_time AS NVARCHAR);
        PRINT 'Total Time (sec): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);
        PRINT '=====================================';

    END TRY

    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'Error occurred during Bronze load';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO

EXEC bronze.load_bronze;

USE DataWarehouse;

--------------------------------------------------
-- CRM CUSTOMER INFO
--------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
    cst_marital_status CHAR(1),
    cst_gndr CHAR(1),
    cst_create_date DATE
);

--------------------------------------------------
-- CRM PRODUCT INFO
--------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(150),
    prd_cost DECIMAL(10,2),
    prd_line NVARCHAR(20),
    prd_start_dt DATE,
    prd_end_dt DATE
);

--------------------------------------------------
-- CRM SALES DETAILS
--------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(20),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,   -- assuming YYYYMMDD format
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales DECIMAL(12,2),
    sls_quantity INT,
    sls_price DECIMAL(12,2)
);

--------------------------------------------------
-- ERP CUSTOMER
--------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(10)
);

--------------------------------------------------
-- ERP LOCATION
--------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(100)
);

--------------------------------------------------
-- ERP PRODUCT CATEGORY
--------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(20),
    cat NVARCHAR(100),
    subcat NVARCHAR(100),
    maintenance NVARCHAR(10)
);

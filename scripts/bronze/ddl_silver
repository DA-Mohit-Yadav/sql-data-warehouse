-- DDL Silver layer

USE DataWarehouse;

--------------------------------------------------
-- CRM CUSTOMER INFO
--------------------------------------------------
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
    cst_marital_status NVARCHAR(20),
    cst_gndr NVARCHAR(20),
    cst_create_date DATE,
    dwh_create_Date date default getdate()
);

--------------------------------------------------
-- CRM PRODUCT INFO
--------------------------------------------------
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(150),
    prd_cost DECIMAL(10,2),
    prd_line NVARCHAR(20),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_Date date default getdate()
);

--------------------------------------------------
-- CRM SALES DETAILS
--------------------------------------------------
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(20),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt Date,   
    sls_ship_dt DAte,
    sls_due_dt Date,
    sls_sales DECIMAL(12,2),
    sls_quantity INT,
    sls_price DECIMAL(12,2),
    dwh_create_Date date default getdate()
);

--------------------------------------------------
-- ERP CUSTOMER
--------------------------------------------------
DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(10),
    dwh_create_Date date default getdate()
);

--------------------------------------------------
-- ERP LOCATION
--------------------------------------------------
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(100),
    dwh_create_Date date default getdate()
);

--------------------------------------------------
-- ERP PRODUCT CATEGORY
--------------------------------------------------
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR(20),
    cat NVARCHAR(100),
    subcat NVARCHAR(100),
    maintenance NVARCHAR(10),
    dwh_create_Date date default getdate()
);

/* ============================================================
   PURPOSE
   ============================================================

   This script creates the GOLD layer views for the DataWarehouse.

   It builds a STAR SCHEMA using views on top of the SILVER layer:

   - gold.dim_customers → Customer dimension (lookup)
   - gold.dim_products  → Product dimension (lookup)
   - gold.fact_sales    → Sales fact table

   Key transformations:
   - Joins CRM and ERP data into unified dimensions
   - Generates surrogate-like keys using ROW_NUMBER()
   - Filters only current product records (prd_end_dt IS NULL)
   - Standardizes gender using fallback logic
   - Creates a denormalized analytical model for reporting

   This layer is designed for:
   - BI tools (Power BI, Tableau)
   - Ad-hoc analytics
   - Reporting queries

   Architecture:
   Bronze → Silver → Gold (Views-based Star Schema)


   ============================================================
   WARNINGS / IMPORTANT NOTES
   ============================================================

   1. VIEW-BASED MODEL (NO DATA STORAGE)
      - All objects are VIEWS, not physical tables
      - Data is recomputed on every query execution
      - Performance depends on underlying Silver tables

   2. NO HISTORICAL TRACKING (NO SCD)
      - History is NOT maintained
      - Only latest snapshot from Silver is visible
      - Changes in source overwrite previous state

   3. SURROGATE KEYS ARE NOT PERSISTENT
      - ROW_NUMBER() is used to generate keys
      - Keys are NOT stable across executions
      - Keys may change if underlying data changes
      - Not suitable for long-term fact-dimension relationships

   4. NOT A TRUE DIMENSIONAL MODEL
      - This is a logical star schema, not physical
      - No primary key / foreign key constraints
      - No indexing or performance optimization

   5. DEPENDENCY ON SILVER LAYER
      - Assumes Silver layer is fully loaded and clean
      - Any schema or logic change in Silver will affect these views

   6. PRODUCT FILTERING LOGIC
      - Only active products are included (prd_end_dt IS NULL)
      - Historical product records are excluded
*/




use DataWarehouse;

--------------------------------------------------------------------------------------------------------------------
-- Dim_Custmers
--------------------------------------------------------------------------------------------------------------------

create or alter view gold.dim_customers as(

SELECT
    row_number() over(order by ci.cst_id) as customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,

    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
   
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid);

--------------------------------------------------------------------------------------------------------------------
-- Dim_Prodcuts
--------------------------------------------------------------------------------------------------------------------

create or alter view gold.dim_products as (
SELECT
    row_number() over(order by pn.prd_id) as product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL) ;  -- filter out all historical data


--------------------------------------------------------------------------------------------------------------------
-- Fact_Sales
--------------------------------------------------------------------------------------------------------------------
create or alter view gold.fact_sales as (
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id) 
    ;

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
===============================================================================
*/

USE DataWarehouse

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key nvarchar(50),
    cst_firstname nvarchar(50),
    cst_lastname nvarchar(50),
    cst_marital_status nvarchar(50),
    cst_gndr nvarchar(50),
    cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost INT,
prd_line nvarchar(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12(
CID nvarchar(50),
bdate DATE,
gen nvarchar(50)
);

IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
CID nvarchar(50),
CNTRY nvarchar(50)
);

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintanance nvarchar(50)
);















 

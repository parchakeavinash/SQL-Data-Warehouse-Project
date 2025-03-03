/*
Script Purpose: 
  This script creates tables in the silver schemas, dropping existing tables
 if they already exist.
 Run this script to re-define the DDL Structure of 'bronze'  Tables

==================================================================
*/
if OBJECT_ID('silver.crm_cust_info','U')  is not null
	Drop table silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id int,
	cst_key NVARCHAR(50),
	cst_first_name NVARCHAR(50),
	cst_last_name NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date Date,
	dwh_create_date Datetime2 Default GETDATE()
);

if OBJECT_ID('silver.crm_prd_info','U')  is not null
	Drop table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id	int,
	prd_key	NVARCHAR(50),
	prd_nm	NVARCHAR(50),
	prd_cost int,
	prd_line NVARCHAR(50),
	prd_start_dt Datetime,
	prd_end_dt Datetime,
	dwh_create_date Datetime2 Default GETDATE()
);

if OBJECT_ID('silver.crm_sales_details','U')  is not null
	Drop table silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num	NVARCHAR(50),
	sls_prd_key	NVARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt INT,
	sls_ship_dt	INT,
	sls_due_dt	INT,
	sls_sales	INT,
	sls_quantitY INT,
	sls_price INT,
	dwh_create_date Datetime2 Default GETDATE()
);

if OBJECT_ID('silver.erp_cust_az12','U')  is not null
	Drop table silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date Datetime2 Default GETDATE()
);

if OBJECT_ID('silver.erp_loc_a101','U')  is not null
	Drop table silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid	NVARCHAR(50),
	cntr NVARCHAR(50),
	dwh_create_date Datetime2 Default GETDATE()
);

if OBJECT_ID('silver.erp_px_cat_g1v2','U')  is not null
	Drop table silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id	NVARCHAR(50),
	cat	NVARCHAR(50),
	subcat	NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date Datetime2 Default GETDATE()
);

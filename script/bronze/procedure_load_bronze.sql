/*
=================================================================
Stored Procedure : Loading Data into Bronze Layer
================================================================
Purpose:
  This SQL Server stored procedure bronze.load_bronze is designed to load data into the bronze layer of the DataWarehouse from CSV files. It follows these steps:
  
  - Truncate existing data in the tables.
  - Bulk insert new data from specified CSV files.
 -  Log load duration for each table and the entire batch.
  - Handle errors using a TRY...CATCH block

usage exaples:
EXEC bronze.load_bronze;
-==================================================================
*/

EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		print('================================================');
		PRINT('Loading Bronze Layer');
		print('================================================');
	
		print('-------------------------------------------------');
		print('LOADING CRM TABLES');
		print('-------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>>>  Truncating  Table : bronze.crm_cust_info');
		Truncate table bronze.crm_cust_info;

		PRINT('>>>  Inserting Data Into: bronze.crm_cust_info');
		Bulk insert bronze.crm_cust_info
		from 'C:\Users\BP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '	Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as NVARCHAR) + 'SECONDS';
		print '-----------------'

		SET @start_time = GETDATE();
		PRINT('>>>  Truncating  Table : bronze.crm_prd_info');
		Truncate table bronze.crm_prd_info;

		PRINT('>>>  Inserting Data Into: bronze.crm_prd_info');
		Bulk insert bronze.crm_prd_info
		from 'C:\Users\BP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '	Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as NVARCHAR) + 'SECONDS';
		print '-----------------'

		SET @start_time = GETDATE();
		PRINT('>>>  Truncating  Table : bronze.crm_sales_details');
		Truncate table bronze.crm_sales_details;

		PRINT('>>>  Inserting Data Into: bronze.crm_sales_details');
		Bulk insert bronze.crm_sales_details
		from 'C:\Users\BP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '	Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as NVARCHAR) + 'SECONDS';
		print '-----------------'

		print('-------------------------------------------------');
		print('LOADING ERP TABLES');
		print('-------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>>>  Truncating  Table : bronze.erp_cust_az12');
		Truncate table bronze.erp_cust_az12;

		PRINT('>>>  Inserting Data Into: bronze.erp_cust_az12');
		Bulk insert bronze.erp_cust_az12
		from 'C:\Users\BP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '	Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as NVARCHAR) + 'SECONDS';
		print '-----------------'

		SET @start_time = GETDATE();
		PRINT('>>>  Truncating  Table : bronze.erp_loc_a101');
		Truncate table bronze.erp_loc_a101;

		PRINT('>>>  Inserting Data Into: bronze.erp_loc_a101');
		Bulk insert bronze.erp_loc_a101
		from 'C:\Users\BP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '	Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as NVARCHAR) + 'SECONDS';
		print '-----------------'


		SET @start_time = GETDATE();
		PRINT('>>>  Truncating  Table : bronze.erp_px_cat_g1v2');
		Truncate table bronze.erp_px_cat_g1v2;

		PRINT('>>>  Inserting Data Into: bronze.erp_px_cat_g1v2');
		Bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\BP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = Getdate();
		print '	Load Duration: ' + cast(Datediff(second, @start_time, @end_time) as NVARCHAR) + 'SECONDS';
		print '-----------------'

		set @batch_end_time = GETDATE();
		print 'Loading Bronze layer Completed!!';
		print 'Total Load Duration: ' + cast(Datediff(second, @batch_start_time, @batch_end_time) as NVARCHAR) + 'SECONDS';
		print '-----------------'

	END TRY
	BEGIN CATCH
	PRINT '============================================'
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_MESSAGE() AS NVARCHAR);
	PRINT '============================================'
	END CATCH
END

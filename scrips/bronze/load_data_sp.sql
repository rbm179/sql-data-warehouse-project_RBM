/*
===========================================================
This Store procedure loads data into the 'bronze' schema from external CSV files.
===========================================================
it performs following actions:
	- Truncated the 'bronze' tables before loading data
	- Uses the 'bulk insert' Command to load data from CSV files to bronze tables

Parameters:
	None.
	
Usage Example:
	EXEC bronze.load_bronze

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME
	DECLARE @start_time DATETIME, @end_time DATETIME
	
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '========================'
		PRINT 'LOADING Bronze Layer'
		PRINT '========================'
		PRINT '------------------------'
		PRINT 'LOADING CRM Tables'
		PRINT '------------------------'
		SET @start_time = GETDATE()
		PRINT 'Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT 'Inserting data into bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Administrator\Desktop\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------'

		set @start_time = GETDATE()
		PRINT 'Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT 'Inserting data into bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Administrator\Desktop\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------'

		SET @start_time = GETDATE()
		PRINT 'Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT 'Inserting data into bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Administrator\Desktop\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------'


		PRINT '------------------------'
		PRINT 'LOADING ERP Tables'
		PRINT '------------------------'
		SET @start_time= GETDATE()
		PRINT 'Truncating Table: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		PRINT 'Inserting data into bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\Administrator\Desktop\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------'

		SET @start_time=GETDATE()
		PRINT 'Truncating Table: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101
		PRINT 'Inserting data into bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\Administrator\Desktop\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		)
		set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------'

		SET @start_time= GETDATE()
		PRINT 'Truncating Table: bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
		PRINT 'Inserting data into bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\Administrator\Desktop\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		set @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'
		PRINT '--------------'

		SET @batch_end_time = GETDATE()
		PRINT 'Batch Processing time duration is ' + cast (DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
	END TRY
	BEGIN CATCH
		PRINT '======================='
		PRINT 'ERROR OCCURED DURIN GLOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' +CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' +CAST (ERROR_STATE()AS NVARCHAR);
		PRINT '======================='
	END CATCH


END

/*

SELECT * FROM  bronze.erp_CUST_AZ12
select count(*) from  bronze.erp_CUST_AZ12

SELECT * FROM bronze.erp_LOC_A101
select count(*) from bronze.erp_LOC_A101

SELECT * FROM bronze.erp_PX_CAT_G1V2
select count(*) from bronze.erp_PX_CAT_G1V2

*/

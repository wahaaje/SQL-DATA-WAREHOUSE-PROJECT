/*
======================================================================================================================================================
Stored Procedure: Bronze Layer (Source -----> Bronze)
======================================================================================================================================================
Scrit Purpose: 
This stored procedure loads data into the 'bronze' schema from external csv files.
It performs the following actions:
  - Truncates the bronze table before loading data.
  - Uses the 'BULK INSERT' command to load data from CSV files.

Parameters:
    None.
  This Stored Procedure doesnot accepts any parameters or return any values.

Using Example:
EXEC bronze.load_bronze;
======================================================================================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE(); 
		PRINT '===================================================================================================';
		PRINT 'Loading the Bronze Layer';
		PRINT '===================================================================================================';


		PRINT '---------------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------------------------------------------------------';


		SET @start_time = GETDATE()
		PRINT 'Truncating Tables: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		PRINT 'Bulk inserting data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\DATA ANALYSIS\SQL\Projects\SQL DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		); 
		SET @end_time = GETDATE()
		PRINT '>> loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '>>------------------';  


		SET @start_time = GETDATE()
		PRINT 'Truncating Tables: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		
		PRINT 'Bulk inserting data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\DATA ANALYSIS\SQL\Projects\SQL DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		); 
		SET @end_time = GETDATE()
		PRINT '>> loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '>>------------------';


		SET @start_time = GETDATE()
		PRINT 'Truncating Tables: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		PRINT 'Bulk inserting data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\DATA ANALYSIS\SQL\Projects\SQL DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '>>------------------';


		PRINT '---------------------------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT 'Truncating Tables: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		PRINT 'Bulk inserting data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\DATA ANALYSIS\SQL\Projects\SQL DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '>>------------------';

		SET @start_time = GETDATE()
		PRINT 'Truncating Tables: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		
		PRINT 'Bulk inserting data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\DATA ANALYSIS\SQL\Projects\SQL DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '>>------------------';

		SET @start_time = GETDATE()
		PRINT 'Truncating Tables: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		 
		PRINT 'Bulk inserting data into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\DATA ANALYSIS\SQL\Projects\SQL DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (

			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> loading duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
		PRINT '>>------------------';

		SET @batch_end_time = GETDATE()
		PRINT '================================================================================================'
		PRINT 'Loading Bronze layer is completed';
		PRINT ' Total Load Duration:  ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds';
		PRINT '================================================================================================'
	END TRY
	BEGIN CATCH
		PRINT '================================================================================================'
		PRINT 'Error occured during loading Bronze layer'
		PRINT 'Error Message' + ERROR_MESSAGE(); 
		PRINT 'Error Message' + cast(ERROR_Number() AS NVARCHAR); 
		PRINT 'Error Message' + cast(ERROR_STATE() AS NVARCHAR); 
		PRINT '================================================================================================'
	END CATCH
END

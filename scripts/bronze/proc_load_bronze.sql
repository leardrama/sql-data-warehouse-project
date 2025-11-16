CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=================================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '=================================';

        PRINT '================================='; 
        PRINT 'LOADING CRM TABLES';
        PRINT '=================================';

        
        SET @start_time = GETDATE();
        PRINT 'Truncating: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT 'Inserting: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/data/datasets Kopie/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------'

        SET @start_time = GETDATE();
        PRINT 'Truncating: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'Inserting: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/data/datasets Kopie/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------'

        SET @start_time = GETDATE();
        PRINT 'Truncating: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT 'Inserting: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/data/datasets Kopie/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------'

        PRINT '=================================';
        PRINT 'LOADING ERP TABLES';
        PRINT '=================================';

        SET @start_time = GETDATE();
        PRINT 'Truncating: bronze.erp_CUST_AZ12';
        TRUNCATE TABLE bronze.erp_CUST_AZ12;

        PRINT 'Inserting: bronze.erp_CUST_AZ12';
        BULK INSERT bronze.erp_CUST_AZ12
        FROM '/data/datasets Kopie/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------'

        SET @start_time = GETDATE();
        PRINT 'Truncating: bronze.erp_LOC_A101';
        TRUNCATE TABLE bronze.erp_LOC_A101;

        PRINT 'Inserting: bronze.erp_LOC_A101';
        BULK INSERT bronze.erp_LOC_A101
        FROM '/data/datasets Kopie/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------'

        SET @start_time = GETDATE();
        PRINT 'Truncating: bronze.erp_PX_CAT_G1V2';
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

        PRINT 'Inserting: bronze.erp_PX_CAT_G1V2';
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM '/data/datasets Kopie/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------'

        SET @batch_end_time = GETDATE();
        PRINT'>> LOAD WHOLE BRONZE LAYER DURATION: ' + CAST(DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
        PRINT '--------------------'

    END TRY
    BEGIN CATCH
        PRINT '===================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_MESSAGE() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===================================';
    END CATCH

    END

    PRINT 'BRONZE LAYER LOADED';
GO


-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    02_bronze_bulk_insert.sql
-- Purpose: Stored procedures to bulk-load CSV files into bronze tables.
--          Called by SSIS Execute SQL Tasks.
-- =============================================================================

-- PROCEDURE: bronze.usp_load_customer_master

CREATE OR ALTER PROCEDURE bronze.usp_load_customer_master
    @file_path   VARCHAR(500),
    @batch_id    INT
AS
BEGIN
    SET NOCOUNT ON;

PRINT 'Batch ID = ' + ISNULL(CAST(@batch_id AS VARCHAR), 'NULL');
PRINT 'File Path = ' + @file_path;

    -- STEP 1: Temp staging table — matches CSV columns EXACTLY (no extras)
    DROP TABLE IF EXISTS #stg_customer;
    CREATE TABLE #stg_customer (
        customer_id          VARCHAR(20),
        customer_name        VARCHAR(200),
        id_number            VARCHAR(20),
        date_of_birth        VARCHAR(20),
        gender               VARCHAR(10),
        employment_status    VARCHAR(50),
        monthly_income       VARCHAR(30),
        marital_status       VARCHAR(30),
        customer_segment     VARCHAR(50),
        province             VARCHAR(50),
        email                VARCHAR(200),
        phone                VARCHAR(30),
        credit_score         VARCHAR(10),
        onboarding_date      VARCHAR(20),
        source_system        VARCHAR(50),
        record_created_at    VARCHAR(30)
    );

    -- STEP 2: Bulk insert into staging (column count matches CSV perfectly)
    BULK INSERT #stg_customer
FROM 'C:\Users\Admin\Desktop\Data Engeneering Projects\Credit risk platform Engineering and Analytics\Credit-risk-platform-Engineering-and-Analytics\raw_data\customer_master.csv'
WITH(
     FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     ROWTERMINATOR = '0x0a',
     MAXERRORS = 0,
     TABLOCK
     ); 
PRINT 'BULK INSERT completed';

SELECT COUNT(*) AS staging_rows FROM #stg_customer;

    -- STEP 3: Insert into bronze table, adding pipeline metadata columns now
    INSERT INTO bronze.customer_master (
        customer_id, customer_name, id_number, date_of_birth, gender,
        employment_status, monthly_income, marital_status, customer_segment,
        province, email, phone, credit_score, onboarding_date,
        source_system, record_created_at,
        load_date, batch_id, source_file, is_duplicate_flag, load_status
    )
    SELECT
        customer_id, customer_name, id_number, date_of_birth, gender,
        employment_status, monthly_income, marital_status, customer_segment,
        province, email, phone, credit_score, onboarding_date,
        source_system, record_created_at,
        GETDATE(), @batch_id, @file_path, 0, 'LOADED'
    FROM #stg_customer
    WHERE NULLIF(LTRIM(RTRIM(customer_id)), '') IS NOT NULL;;  -- discard blank trailing rows

    -- STEP 4: Flag duplicates within this batch
   ;WITH dupes (customer_id, rn) AS
(
    SELECT
        customer_id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY customer_id
        )
    FROM bronze.customer_master
    WHERE batch_id = @batch_id
)
UPDATE b
SET is_duplicate_flag = 1
FROM bronze.customer_master b
JOIN dupes d
    ON b.customer_id = d.customer_id
WHERE b.batch_id = @batch_id
  AND d.rn > 1;
    DECLARE @loaded INT = (SELECT COUNT(*) FROM bronze.customer_master
                           WHERE batch_id = @batch_id);
    PRINT CONCAT('bronze.customer_master loaded: ', @loaded,
                 ' rows | Batch: ', @batch_id);
END;
GO

EXEC [bronze].[usp_load_customer_master]
    @file_path = 'C:\file.csv',
    @batch_id = 1;

SELECT * FROM bronze.customer_master



-- =============================================================================
-- HOW TO CALL (run this after deploying the procedures above)
-- Update the file paths to match where your CSVs live on the SQL Server.
-- =============================================================================
/*
DECLARE @batch_id INT;
EXEC audit.usp_start_batch
    @triggered_by     = 'MANUAL',
    @environment      = 'DEV',
    @pipeline_version = '1.0',
    @batch_id         = @batch_id OUTPUT;

EXEC bronze.usp_load_customer_master
    @file_path = 'C:\CreditRisk\data\customer_master.csv',
    @batch_id  = @batch_id;

EXEC bronze.usp_load_loan_accounts
    @file_path = 'C:\CreditRisk\data\loan_accounts.csv',
    @batch_id  = @batch_id;

EXEC bronze.usp_load_repayments
    @file_path            = 'C:\CreditRisk\data\repayments.csv',
    @batch_id             = @batch_id,
    @last_successful_load = '1900-01-01';   -- use '1900-01-01' for first full load

EXEC bronze.usp_load_collections
    @file_path = 'C:\CreditRisk\data\collections.csv',
    @batch_id  = @batch_id;

EXEC bronze.usp_load_defaults
    @file_path = 'C:\CreditRisk\data\defaults.csv',
    @batch_id  = @batch_id;
*/
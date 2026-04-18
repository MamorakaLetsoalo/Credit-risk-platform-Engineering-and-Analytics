-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    10_repayments_bulk_insert.sql
-- Purpose: Stored procedures to bulk-load CSV files into bronze repayments tables.
--          Called by SSIS Execute SQL Tasks.
-- =============================================================================

-- PROCEDURE: bronze.usp_load_repayments  (INCREMENTAL)
--Only loads records newer than the last successful load date



CREATE OR ALTER PROCEDURE bronze.usp_load_repayments_incremental
    @file_path              VARCHAR(500),
    @batch_id               INT,
    @last_successful_load   DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    -- STEP 1: STAGING TABLE
    DROP TABLE IF EXISTS #stg_repayment;

    CREATE TABLE #stg_repayment (
        repayment_id      VARCHAR(20),
        loan_id           VARCHAR(20),
        payment_date      VARCHAR(20),
        due_date          VARCHAR(20),
        payment_amount    VARCHAR(30),
        expected_amount   VARCHAR(30),
        days_past_due     VARCHAR(10),
        repayment_status  VARCHAR(30),
        payment_method    VARCHAR(50),
        source_system     VARCHAR(50),
        record_created_at VARCHAR(30)
    );

    -- STEP 2: BULK INSERT INTO STAGING
    BULK INSERT #stg_repayment
    FROM 'C:\Users\Admin\Desktop\Data Engeneering Projects\Credit risk platform Engineering and Analytics\Credit-risk-platform-Engineering-and-Analytics\raw_data\repayments.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        MAXERRORS = 0,
        TABLOCK
    );

    PRINT 'Repayments staging load complete';

    -- STEP 3: INSERT INTO BRONZE (WITH METADATA)
    INSERT INTO bronze.repayments (
        repayment_id, loan_id, payment_date, due_date,
        payment_amount, expected_amount, days_past_due,
        repayment_status, payment_method,
        source_system, record_created_at,
        load_date, batch_id, source_file,
        is_duplicate_flag, load_status
    )
    SELECT
        repayment_id,
        loan_id,
        payment_date,
        due_date,
        payment_amount,
        expected_amount,
        days_past_due,
        repayment_status,
        payment_method,
        source_system,
        record_created_at,
        GETDATE(),
        @batch_id,
        @file_path,
        0,
        'LOADED'
    FROM #stg_repayment
    WHERE NULLIF(LTRIM(RTRIM(repayment_id)), '') IS NOT NULL
      AND repayment_id IS NOT NULL
      AND TRY_CONVERT(DATETIME2, record_created_at) > @last_successful_load;

    -- STEP 4: DUPLICATE FLAGGING (BATCH SAFE)
    ;WITH dupes (repayment_id, rn) AS (
        SELECT
            repayment_id,
            ROW_NUMBER() OVER (
                PARTITION BY repayment_id
                ORDER BY repayment_id
            )
        FROM bronze.repayments
        WHERE batch_id = @batch_id
    )
    UPDATE b
    SET is_duplicate_flag = 1
    FROM bronze.repayments b
    JOIN dupes d
        ON b.repayment_id = d.repayment_id
    WHERE b.batch_id = @batch_id
      AND d.rn > 1;

    -- STEP 5: LOGGING
    DECLARE @loaded INT =
    (
        SELECT COUNT(*)
        FROM bronze.repayments
        WHERE batch_id = @batch_id
    );

    PRINT CONCAT('repayments loaded | Batch: ', @batch_id, ' | Rows: ', @loaded);
END;
GO

EXEC bronze.usp_load_repayments_incremental
    @file_path = 'C:\data\repayments.csv',
    @batch_id = 1,
    @last_successful_load = '1900-01-01';

SELECT * FROM [bronze].[repayments];
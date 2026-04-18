-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    10_defaults_bulk_insert.sql
-- Purpose: Stored procedures to bulk-load CSV files into bronze defaults tables.
--          Called by SSIS Execute SQL Tasks.
-- =============================================================================

-- PROCEDURE: bronze.usp_load_defaults

CREATE OR ALTER PROCEDURE bronze.usp_load_defaults
    @file_path VARCHAR(500),
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS #stg_defaults;
    CREATE TABLE #stg_defaults (
        default_id                VARCHAR(20),
        loan_id                   VARCHAR(20),
        default_date              VARCHAR(20),
        default_reason            VARCHAR(100),
        outstanding_at_default    VARCHAR(30),
        recovery_amount           VARCHAR(30),
        recovery_status           VARCHAR(50),
        days_to_default           VARCHAR(10),
        npl_classification        VARCHAR(30),
        provision_amount          VARCHAR(30),
        source_system             VARCHAR(50),
        record_created_at         VARCHAR(30)
    );

    BULK INSERT #stg_defaults
    FROM 'C:\Users\Admin\Desktop\Data Engeneering Projects\Credit risk platform Engineering and Analytics\Credit-risk-platform-Engineering-and-Analytics\raw_data\defaults.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        MAXERRORS = 0,
        TABLOCK
    );

    INSERT INTO bronze.defaults (
        default_id, loan_id, default_date, default_reason,
        outstanding_at_default, recovery_amount, recovery_status,
        days_to_default, npl_classification, provision_amount,
        source_system, record_created_at,
        load_date, batch_id, source_file, is_duplicate_flag, load_status
    )
    SELECT
        default_id, loan_id, default_date, default_reason,
        outstanding_at_default, recovery_amount, recovery_status,
        days_to_default, npl_classification, provision_amount,
        source_system, record_created_at,
        GETDATE(), @batch_id, @file_path, 0, 'LOADED'
    FROM #stg_defaults
    WHERE NULLIF(LTRIM(RTRIM(default_id)), '') IS NOT NULL;

    PRINT CONCAT('defaults loaded | Batch: ', @batch_id);
END;
GO

EXEC [bronze].[usp_load_defaults]
    @file_path = 'C:\file.csv',
    @batch_id = 1;

SELECT * FROM [bronze].[defaults];
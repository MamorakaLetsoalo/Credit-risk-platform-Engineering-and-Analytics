-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    09_collections_bulk_insert.sql
-- Purpose: Stored procedures to bulk-load CSV files into bronze loan_accounts tables.
--          Called by SSIS Execute SQL Tasks.
-- =============================================================================

-- PROCEDURE: bronze.usp_load_collections

CREATE OR ALTER PROCEDURE bronze.usp_load_collections
    @file_path VARCHAR(500),
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS #stg_collections;
    CREATE TABLE #stg_collections (
        collection_id        VARCHAR(20),
        loan_id              VARCHAR(20),
        collector_id         VARCHAR(20),
        action_date          VARCHAR(20),
        action_type          VARCHAR(50),
        outcome              VARCHAR(50),
        promised_amount      VARCHAR(30),
        next_action_date     VARCHAR(20),
        escalation_level     VARCHAR(20),
        source_system        VARCHAR(50),
        record_created_at    VARCHAR(30)
    );

    BULK INSERT #stg_collections
    FROM 'C:\Users\Admin\Desktop\Data Engeneering Projects\Credit risk platform Engineering and Analytics\Credit-risk-platform-Engineering-and-Analytics\raw_data\collections.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        MAXERRORS = 0,
        TABLOCK
    );

    INSERT INTO bronze.collections (
        collection_id, loan_id, collector_id, action_date, action_type,
        outcome, promised_amount, next_action_date, escalation_level,
        source_system, record_created_at,
        load_date, batch_id, source_file, is_duplicate_flag, load_status
    )
    SELECT
        collection_id, loan_id, collector_id, action_date, action_type,
        outcome, promised_amount, next_action_date, escalation_level,
        source_system, record_created_at,
        GETDATE(), @batch_id, @file_path, 0, 'LOADED'
    FROM #stg_collections
    WHERE NULLIF(LTRIM(RTRIM(collection_id)), '') IS NOT NULL;

    PRINT CONCAT('collections loaded | Batch: ', @batch_id);
END;
GO

EXEC [bronze].[usp_load_collections]
    @file_path = 'C:\file.csv',
    @batch_id = 1;

SELECT * FROM [bronze].[collections];
-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    08_loan_acount_bulk_insert.sql
-- Purpose: Stored procedures to bulk-load CSV files into bronze loan_accounts tables.
--          Called by SSIS Execute SQL Tasks.
-- =============================================================================

-- PROCEDURE: bronze.usp_load_loan_accounts
CREATE OR ALTER PROCEDURE bronze.usp_load_loan_accounts
    @file_path VARCHAR(500),
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS #stg_loan;
    CREATE TABLE #stg_loan (
        loan_id              VARCHAR(20),
        customer_id          VARCHAR(20),
        loan_type            VARCHAR(50),
        loan_amount          VARCHAR(30),
        interest_rate        VARCHAR(15),
        loan_term_months     VARCHAR(10),
        loan_start_date      VARCHAR(20),
        loan_end_date        VARCHAR(20),
        current_balance      VARCHAR(30),
        loan_status          VARCHAR(30),
        origination_branch   VARCHAR(20),
        collateral_type      VARCHAR(50),
        source_system        VARCHAR(50),
        record_created_at    VARCHAR(30)
    );

    BULK INSERT #stg_loan
    FROM 'C:\Users\Admin\Desktop\Data Engeneering Projects\Credit risk platform Engineering and Analytics\Credit-risk-platform-Engineering-and-Analytics\raw_data\loan_accounts.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        MAXERRORS = 0,
        TABLOCK
    );

    INSERT INTO bronze.loan_accounts (
        loan_id, customer_id, loan_type, loan_amount, interest_rate,
        loan_term_months, loan_start_date, loan_end_date, current_balance,
        loan_status, origination_branch, collateral_type,
        source_system, record_created_at,
        load_date, batch_id, source_file, is_duplicate_flag, load_status
    )
    SELECT
        loan_id, customer_id, loan_type, loan_amount, interest_rate,
        loan_term_months, loan_start_date, loan_end_date, current_balance,
        loan_status, origination_branch, collateral_type,
        source_system, record_created_at,
        GETDATE(), @batch_id, @file_path, 0, 'LOADED'
    FROM #stg_loan
    WHERE NULLIF(LTRIM(RTRIM(loan_id)), '') IS NOT NULL;

    ;WITH dupes (loan_id, rn) AS (
        SELECT loan_id,
               ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY loan_id)
        FROM bronze.loan_accounts
        WHERE batch_id = @batch_id
    )
    UPDATE b
    SET is_duplicate_flag = 1
    FROM bronze.loan_accounts b
    JOIN dupes d ON b.loan_id = d.loan_id
    WHERE b.batch_id = @batch_id
      AND d.rn > 1;

    PRINT CONCAT('loan_accounts loaded | Batch: ', @batch_id);
END;
GO

EXEC [bronze].[usp_load_loan_accounts]
    @file_path = 'C:\file.csv',
    @batch_id = 1;

SELECT * FROM [bronze].[loan_accounts];
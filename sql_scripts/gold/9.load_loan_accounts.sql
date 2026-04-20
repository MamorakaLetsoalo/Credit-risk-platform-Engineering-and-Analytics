-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    02_gold_scd2_and_loads.sql
-- Purpose: SCD Type 2 for dim_customer + load procedures for all dimensions
--          and the fact table with full IFRS9 / Basel metric calculations.
-- =============================================================================

-- PROCEDURE: gold.usp_load_dim_loan

CREATE OR ALTER PROCEDURE gold.usp_load_dim_loan
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    MERGE gold.dim_loan AS tgt
    USING (
    SELECT
        la.loan_id,
        la.loan_type,
        la.loan_amount,
        la.interest_rate,
        la.loan_term_months,
        la.loan_start_date,
        la.loan_end_date,
        la.loan_status,
        la.origination_branch,
        la.collateral_type,

        CASE
            WHEN la.loan_amount < 10000 THEN 'Micro'
            WHEN la.loan_amount < 100000 THEN 'Small'
            WHEN la.loan_amount < 500000 THEN 'Medium'
            WHEN la.loan_amount < 2000000 THEN 'Large'
            ELSE 'Mega'
        END AS loan_size_band,

        CASE
            WHEN la.interest_rate < 10 THEN 'Low'
            WHEN la.interest_rate < 16 THEN 'Standard'
            WHEN la.interest_rate < 22 THEN 'High'
            ELSE 'Very High'
        END AS interest_rate_band,

        CASE
            WHEN la.collateral_type NOT IN ('None','') THEN 1
            ELSE 0
        END AS is_secured

    FROM silver.loan_accounts la
    WHERE la.batch_id = @batch_id
) AS src
    ON tgt.loan_id = src.loan_id
    WHEN MATCHED THEN
        UPDATE SET tgt.loan_status = src.loan_status, tgt.batch_id = batch_id
    WHEN NOT MATCHED THEN
        INSERT (loan_id, loan_type, loan_amount, interest_rate, loan_term_months,
                loan_start_date, loan_end_date, loan_status, origination_branch,
                collateral_type, loan_size_band, interest_rate_band, is_secured, batch_id)
        VALUES (src.loan_id, src.loan_type, src.loan_amount, src.interest_rate,
                src.loan_term_months, src.loan_start_date, src.loan_end_date,
                src.loan_status, src.origination_branch, src.collateral_type,
                src.loan_size_band, src.interest_rate_band, src.is_secured, @batch_id);

    PRINT CONCAT('gold.dim_loan loaded | Batch: ', @batch_id);
END;

EXECUTE gold.usp_load_dim_loan
 @batch_id = 1;

 SELECT * FROM [gold].[dim_loan];
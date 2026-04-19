-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    06_silver_transformations.sql
-- Purpose: ETL stored procedures: cleanse, cast, validate, deduplicate.
--          All data quality rules enforced here before Gold promotion.
-- =============================================================================

-- PROCEDURE: silver.usp_transform_loans

CREATE OR ALTER PROCEDURE silver.usp_transform_loans
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    MERGE silver.loan_accounts AS tgt
    USING (
        SELECT
            loan_id,
            customer_id,
            LOWER(LTRIM(RTRIM(loan_type)))                    AS loan_type,
            CASE
            WHEN TRY_CAST(loan_amount AS DECIMAL(38,2)) > 999999999999
            THEN NULL
             ELSE TRY_CAST(loan_amount AS DECIMAL(38,2))
            END                                               AS loan_amount,
            
            CASE
            WHEN TRY_CAST(interest_rate AS DECIMAL(38,2)) > 100
            THEN NULL
            ELSE TRY_CAST(interest_rate AS DECIMAL(18,2))
            END AS interest_rate,
            TRY_CAST(loan_term_months AS INT)                  AS loan_term_months,
            TRY_CAST(loan_start_date AS DATE)                  AS loan_start_date,
            TRY_CAST(loan_end_date AS DATE)                    AS loan_end_date,
            TRY_CAST(current_balance AS DECIMAL(38,2))         AS current_balance,
            LOWER(LTRIM(RTRIM(loan_status)))                   AS loan_status,
            origination_branch,
            LOWER(LTRIM(RTRIM(collateral_type)))               AS collateral_type,
            source_system,
            batch_id,

            -- DQ FLAGS
                                            
            CASE WHEN NOT EXISTS (
            SELECT 1
            FROM silver.customer_master cm
            WHERE cm.customer_id = deduped.customer_id
            )
            THEN 1 ELSE 0 END                                  AS dq_orphan_customer,
            
            CASE
    WHEN TRY_CAST(current_balance AS DECIMAL(38,2)) >
         TRY_CAST(loan_amount AS DECIMAL(38,2)) * 1.30
    THEN 1
    ELSE 0
END AS dq_balance_exceeds_amount,

            CASE WHEN TRY_CAST(loan_start_date AS DATE) >=
                      TRY_CAST(loan_end_date AS DATE)
                 THEN 1 ELSE 0 END                             AS dq_date_mismatch

        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY load_date DESC) AS rn
            FROM   bronze.loan_accounts la
            WHERE  batch_id = @batch_id
              AND  loan_id IS NOT NULL
        ) deduped
        WHERE rn = 1
    ) AS src
    ON tgt.loan_id = src.loan_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.current_balance          = src.current_balance,
            tgt.loan_status              = src.loan_status,
            tgt.dq_orphan_customer       = src.dq_orphan_customer,
            tgt.dq_balance_exceeds_amount= src.dq_balance_exceeds_amount,
            tgt.batch_id                 = src.batch_id,
            tgt.silver_load_date         = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (loan_id, customer_id, loan_type, loan_amount, interest_rate, loan_term_months,
                loan_start_date, loan_end_date, current_balance, loan_status,
                origination_branch, collateral_type,
                dq_orphan_customer, dq_balance_exceeds_amount, dq_date_mismatch,
                source_system, batch_id)
        VALUES (src.loan_id, src.customer_id, src.loan_type, src.loan_amount,
                src.interest_rate, src.loan_term_months, src.loan_start_date,
                src.loan_end_date, src.current_balance, src.loan_status,
                src.origination_branch, src.collateral_type,
                src.dq_orphan_customer, src.dq_balance_exceeds_amount, src.dq_date_mismatch,
                src.source_system, src.batch_id);

    PRINT CONCAT('silver.loan_accounts processed | Batch: ', @batch_id);
END;

--EXECUTE
EXEC [silver].[usp_transform_loans]
    @batch_id = 1;

SELECT * FROM [silver].[loan_accounts];


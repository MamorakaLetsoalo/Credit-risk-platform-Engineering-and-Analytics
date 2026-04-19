-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    06_silver_transformations.sql
-- Purpose: ETL stored procedures: cleanse, cast, validate, deduplicate.
--          All data quality rules enforced here before Gold promotion.
-- =============================================================================

-- PROCEDURE: silver.usp_transform__repayments (INCREMENTAL)

CREATE OR ALTER PROCEDURE silver.usp_transform_repayments
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.repayments (
        repayment_id, loan_id, payment_date, due_date, payment_amount,
        expected_amount, days_past_due, repayment_status, payment_method,
        dq_orphan_loan, dq_future_payment, source_system, batch_id
    )
    SELECT
        repayment_id,
        loan_id,
        TRY_CAST(payment_date AS DATE),
        TRY_CAST(due_date AS DATE),
        ABS(TRY_CAST(payment_amount AS DECIMAL(38,2))),
        ABS(TRY_CAST(expected_amount AS DECIMAL(38,2))),
        ISNULL(TRY_CAST(days_past_due AS INT), 0),
        LOWER(LTRIM(RTRIM(repayment_status))),
        LOWER(LTRIM(RTRIM(payment_method))),

        CASE WHEN NOT EXISTS (
            SELECT 1 FROM silver.loan_accounts la WHERE la.loan_id = loan_id
        ) THEN 1 ELSE 0 END,

        CASE WHEN TRY_CAST(payment_date AS DATE) > CAST(GETDATE() AS DATE)
             THEN 1 ELSE 0 END,

        source_system,
        batch_id
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY repayment_id ORDER BY load_date DESC) AS rn
        FROM   bronze.repayments b
        WHERE  batch_id = @batch_id
          AND  repayment_id IS NOT NULL
    ) deduped
    WHERE rn = 1
      AND NOT EXISTS (
          SELECT 1 FROM silver.repayments sr WHERE sr.repayment_id = deduped.repayment_id
      );

    PRINT CONCAT('silver.repayments processed | Batch: ', @batch_id);
END;

--EXECUTE
EXEC [silver].[usp_transform_repayments]
    @batch_id = 1;

SELECT * FROM [silver].[repayments]
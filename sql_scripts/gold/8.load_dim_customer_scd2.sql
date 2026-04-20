-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    02_gold_scd2_and_loads.sql
-- Purpose: SCD Type 2 for dim_customer + load procedures for all dimensions
--          and the fact table with full IFRS9 / Basel metric calculations.
-- =============================================================================


-- Implements full SCD Type 2 using MERGE pattern.
-- Tracked attributes: monthly_income, employment_status, customer_segment, credit_score

CREATE OR ALTER PROCEDURE gold.usp_load_dim_customer_scd2
    @batch_id       INT,
    @load_date      DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @load_date IS NULL SET @load_date = CAST(GETDATE() AS DATE);

    -- ── STEP 1: Expire rows where tracked attributes have changed ────────────
    UPDATE gold.dim_customer
    SET    expiry_date   = DATEADD(DAY, -1, @load_date),
           is_current    = 0
    FROM   gold.dim_customer dc
    JOIN   silver.customer_master src ON src.customer_id = dc.customer_id
    WHERE  dc.is_current = 1
      AND  (
            dc.monthly_income    <> src.monthly_income
         OR dc.employment_status <> src.employment_status
         OR dc.customer_segment  <> src.customer_segment
         OR ISNULL(dc.credit_score, -1) <> ISNULL(src.credit_score, -1)
           );

    -- ── STEP 2: Insert new versions for changed records ──────────────────────
    INSERT INTO gold.dim_customer (
        customer_id, customer_name, id_number, date_of_birth, gender,
        employment_status, monthly_income, marital_status, customer_segment,
        province, credit_score, onboarding_date,
        income_band, credit_risk_band,
        effective_date, expiry_date, is_current, version_number, batch_id
    )
    SELECT
        src.customer_id,
        src.customer_name,
        src.id_number,
        src.date_of_birth,
        src.gender,
        src.employment_status,
        src.monthly_income,
        src.marital_status,
        src.customer_segment,
        src.province,
        src.credit_score,
        src.onboarding_date,
        gold.fn_income_band(src.monthly_income),
        gold.fn_credit_risk_band(ISNULL(src.credit_score, 400)),
        @load_date,
        '9999-12-31',
        1,
        ISNULL((
            SELECT MAX(version_number) + 1
            FROM   gold.dim_customer prev
            WHERE  prev.customer_id = src.customer_id
        ), 1),
        @batch_id
    FROM silver.customer_master src
    -- Only insert if: brand new customer OR was just expired
    WHERE NOT EXISTS (
        SELECT 1 FROM gold.dim_customer dc
        WHERE  dc.customer_id = src.customer_id
          AND  dc.is_current  = 1
    );

    DECLARE @rows INT = @@ROWCOUNT;
    PRINT CONCAT('gold.dim_customer SCD2 applied: ', @rows, ' new/updated rows | Batch: ', @batch_id);
END;

EXEC gold.usp_load_dim_customer_scd2
    @batch_id = 1001;

--view the table the stored procedure loads
    SELECT *
FROM gold.dim_customer;


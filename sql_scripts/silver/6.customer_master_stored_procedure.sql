-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    06_silver_transformations.sql
-- Purpose: ETL stored procedures: cleanse, cast, validate, deduplicate.
--          All data quality rules enforced here before Gold promotion.
-- =============================================================================

-- PROCEDURE: silver.usp_transform_customers
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE silver.usp_transform_customers
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    -- STEP 1: Upsert (MERGE) cleansed records
    MERGE silver.customer_master AS tgt
    USING (
        SELECT
            customer_id,
            LTRIM(RTRIM(customer_name))                                     AS customer_name,
            LTRIM(RTRIM(id_number))                                         AS id_number,
            TRY_CAST(date_of_birth AS DATE)                                 AS date_of_birth,
            CASE UPPER(LEFT(LTRIM(gender),1))
                WHEN 'M' THEN 'M' WHEN 'F' THEN 'F' ELSE 'U' END          AS gender,
            LOWER(LTRIM(RTRIM(employment_status)))                          AS employment_status,
            ABS(TRY_CAST(monthly_income AS DECIMAL(18,2)))                  AS monthly_income,
            LOWER(LTRIM(RTRIM(marital_status)))                             AS marital_status,
            LOWER(LTRIM(RTRIM(customer_segment)))                           AS customer_segment,
            LOWER(LTRIM(RTRIM(province)))                                   AS province,
            LOWER(LTRIM(RTRIM(email)))                                      AS email,
            LTRIM(RTRIM(phone))                                             AS phone,
            TRY_CAST(credit_score AS SMALLINT)                             AS credit_score,
            TRY_CAST(onboarding_date AS DATE)                              AS onboarding_date,
            source_system,
            batch_id,

            -- DQ FLAGS
            CASE WHEN TRY_CAST(monthly_income AS DECIMAL(18,2)) IS NULL
                   OR TRY_CAST(monthly_income AS DECIMAL(18,2)) < 0
                   OR TRY_CAST(monthly_income AS DECIMAL(18,2)) > 5000000
                 THEN 1 ELSE 0 END                                          AS dq_income_flag,

            CASE WHEN email NOT LIKE '%@%.%'
                 THEN 1 ELSE 0 END                                          AS dq_email_flag,

            CASE WHEN TRY_CAST(date_of_birth AS DATE) > CAST(GETDATE() AS DATE)
                   OR TRY_CAST(date_of_birth AS DATE) < '1900-01-01'
                 THEN 1 ELSE 0 END                                          AS dq_dob_flag

        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY load_date DESC) AS rn
            FROM   bronze.customer_master
            WHERE  batch_id = @batch_id  
           AND  customer_id IS NOT NULL
        ) deduped
        WHERE rn = 1
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.customer_name     = src.customer_name,
            tgt.monthly_income    = src.monthly_income,
            tgt.employment_status = src.employment_status,
            tgt.customer_segment  = src.customer_segment,
            tgt.credit_score      = src.credit_score,
            tgt.dq_income_flag    = src.dq_income_flag,
            tgt.dq_email_flag     = src.dq_email_flag,
            tgt.dq_dob_flag       = src.dq_dob_flag,
            tgt.batch_id          = src.batch_id,
            tgt.silver_load_date  = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_name, id_number, date_of_birth, gender,
                employment_status, monthly_income, marital_status, customer_segment,
                province, email, phone, credit_score, onboarding_date,
                dq_income_flag, dq_email_flag, dq_dob_flag, source_system, batch_id)
        VALUES (src.customer_id, src.customer_name, src.id_number, src.date_of_birth,
                src.gender, src.employment_status, src.monthly_income, src.marital_status,
                src.customer_segment, src.province, src.email, src.phone,
                src.credit_score, src.onboarding_date, src.dq_income_flag,
                src.dq_email_flag, src.dq_dob_flag, src.source_system, src.batch_id);

    DECLARE @inserted INT = @@ROWCOUNT;
    PRINT CONCAT('silver.customer_master: ', @inserted, ' rows processed | Batch: ', @batch_id);
END;


--EXECUTE
EXEC [silver].[usp_transform_customers]
    @batch_id = 1;

SELECT * FROM [silver].[customer_master];




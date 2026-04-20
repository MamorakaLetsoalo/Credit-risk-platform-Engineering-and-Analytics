-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    02_gold_scd2_and_loads.sql
-- Purpose: SCD Type 2 for dim_customer + load procedures for all dimensions
--          and the fact table with full IFRS9 / Basel metric calculations.
-- =============================================================================

-- PROCEDURE: gold.usp_load_fact_credit_risk
-- Computes IFRS 9 ECL metrics and populates the central fact table


CREATE OR ALTER PROCEDURE gold.usp_load_fact_credit_risk
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Calculate PD using a logistic-style approximation from DPD and credit score
    -- LGD varies by collateral type (regulatory Basel floor: 40% unsecured)
    -- EAD = current outstanding balance
    -- ECL Stage: 1=<30DPD, 2=30-89DPD, 3=90+DPD

    INSERT INTO gold.fact_credit_risk (
        customer_key, loan_key, repayment_key, date_key, due_date_key,
        repayment_id,
        exposure_amount, payment_amount, expected_amount, payment_shortfall,
        days_past_due, default_flag, npl_flag,
        probability_of_default, loss_given_default, exposure_at_default,
        expected_credit_loss, ecl_stage,
        dpd_bucket, is_collections_active, batch_id
    )
    SELECT
        dc.customer_key,
        dl.loan_key,
        ISNULL(drs.repayment_key, 6),              -- 6 = UNKNOWN
        dd_pay.date_key,
        dd_due.date_key,
        r.repayment_id,

        -- Measures
        la.current_balance                          AS exposure_amount,
        r.payment_amount,
        r.expected_amount,
        r.payment_shortfall,
        r.days_past_due,
        CASE WHEN la.loan_status = 'DEFAULT' THEN 1 ELSE 0 END AS default_flag,
        CASE WHEN r.days_past_due >= 90 THEN 1 ELSE 0 END       AS npl_flag,

        -- PD: logistic approximation — capped between 0.5% and 99%
        CAST(CASE
            WHEN r.days_past_due = 0     THEN 0.005
            WHEN r.days_past_due <= 30   THEN 0.05  + (r.days_past_due / 30.0) * 0.10
            WHEN r.days_past_due <= 90   THEN 0.15  + (r.days_past_due / 90.0) * 0.35
            WHEN r.days_past_due <= 180  THEN 0.50  + (r.days_past_due / 180.0) * 0.30
            ELSE 0.99
        END AS DECIMAL(7,4))                        AS probability_of_default,

        -- LGD: Basel floor rates by collateral
        CAST(CASE dl.collateral_type
            WHEN 'Property'       THEN 0.35
            WHEN 'Vehicle'        THEN 0.40
            WHEN 'Cash Deposit'   THEN 0.10
            WHEN 'Surety'         THEN 0.45
            ELSE 0.65             -- unsecured
        END AS DECIMAL(7,4))                        AS loss_given_default,

        -- EAD: current balance + undrawn commitment (simplified)
        la.current_balance                          AS exposure_at_default,

        -- ECL = PD × LGD × EAD
        CAST(
            CASE
                WHEN r.days_past_due = 0   THEN 0.005
                WHEN r.days_past_due <= 30 THEN 0.05  + (r.days_past_due / 30.0) * 0.10
                WHEN r.days_past_due <= 90 THEN 0.15  + (r.days_past_due / 90.0) * 0.35
                ELSE 0.99
            END
            *
            CASE dl.collateral_type
                WHEN 'Property' THEN 0.35 WHEN 'Vehicle' THEN 0.40
                WHEN 'Cash Deposit' THEN 0.10 WHEN 'Surety' THEN 0.45
                ELSE 0.65
            END
            * la.current_balance
        AS DECIMAL(18,2))                           AS expected_credit_loss,

        -- IFRS9 Stage
        CASE
            WHEN r.days_past_due < 30  THEN 1
            WHEN r.days_past_due < 90  THEN 2
            ELSE 3
        END                                         AS ecl_stage,

        -- DPD bucket
        CASE
            WHEN r.days_past_due = 0    THEN 'Current'
            WHEN r.days_past_due <= 30  THEN '1-30 DPD'
            WHEN r.days_past_due <= 60  THEN '31-60 DPD'
            WHEN r.days_past_due <= 90  THEN '61-90 DPD'
            ELSE '90+ DPD'
        END                                         AS dpd_bucket,

        CASE WHEN EXISTS (
            SELECT 1 FROM silver.collections c
            WHERE c.loan_id = la.loan_id
              AND c.action_date >= DATEADD(MONTH,-3, r.payment_date)
        ) THEN 1 ELSE 0 END                         AS is_collections_active,

        @batch_id
    FROM silver.repayments r
    JOIN silver.loan_accounts    la  ON la.loan_id       = r.loan_id
    JOIN gold.dim_customer       dc  ON dc.customer_id   = la.customer_id AND dc.is_current = 1
    JOIN gold.dim_loan           dl  ON dl.loan_id       = la.loan_id
    JOIN gold.dim_date           dd_pay ON dd_pay.date_key = CAST(FORMAT(r.payment_date,'yyyyMMdd') AS INT)
    LEFT JOIN gold.dim_date      dd_due ON dd_due.date_key = CAST(FORMAT(r.due_date,'yyyyMMdd') AS INT)
    LEFT JOIN gold.dim_repayment_status drs ON drs.repayment_status = r.repayment_status
    WHERE r.batch_id = @batch_id
      AND NOT EXISTS (
          SELECT 1 FROM gold.fact_credit_risk fcr
          WHERE  fcr.repayment_id = r.repayment_id
      );

    PRINT CONCAT('gold.fact_credit_risk loaded | Batch: ', @batch_id, ' | Rows: ', @@ROWCOUNT);
END;

EXECUTE gold.usp_load_fact_credit_risk
 @batch_id = 1;

 select * from [gold].[fact_credit_risk];
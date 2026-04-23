-- =============================================================================
-- CREDIT RISK PLATFORM | AUDIT & CONTROL LAYER
-- File:    08_audit_control_tables.sql
-- Purpose: ETL control table, audit log, data quality summary, error log.
--          These tables power the observability and operational monitoring
--          of the entire pipeline.
-- =============================================================================


--STORED PROCEDURES: Audit Helpers (called from SSIS tasks)
-- Run ALL data quality checks for a batch
CREATE OR ALTER PROCEDURE audit.usp_run_dq_checks
    @batch_id INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @total INT, @failed INT;

    -- DQ Check 1: Null customer_id in bronze
    SELECT @total = COUNT(*), @failed = SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)
    FROM bronze.customer_master WHERE batch_id = @batch_id;
    INSERT INTO audit.data_quality_log
        (batch_id, layer, table_name, dq_rule_name, dq_rule_description,
         total_rows_checked, rows_passed, rows_failed, threshold_pct, dq_status)
    VALUES (@batch_id, 'BRONZE', 'customer_master', 'NULL_CUSTOMER_ID',
            'customer_id must not be null',
            @total, @total - @failed, @failed, 0.0,
            CASE WHEN @failed = 0 THEN 'PASS' ELSE 'FAIL' END);

    -- DQ Check 2: Orphan loans (no matching customer)
    SELECT @total = COUNT(*),
       @failed = ISNULL(SUM(CAST(dq_orphan_customer AS INT)),0)
    FROM silver.loan_accounts
    WHERE batch_id = @batch_id;
    INSERT INTO audit.data_quality_log
        (batch_id, layer, table_name, dq_rule_name, dq_rule_description,
         total_rows_checked, rows_passed, rows_failed, threshold_pct, dq_status)
    VALUES (@batch_id, 'SILVER', 'loan_accounts', 'ORPHAN_CUSTOMER_LOANS',
            'Loans must have a matching customer_id in silver.customer_master',
            @total, @total - @failed, @failed, 1.0,
            CASE WHEN CAST(@failed AS FLOAT)/@total*100 <= 1.0 THEN 'PASS'
                 WHEN CAST(@failed AS FLOAT)/@total*100 <= 5.0 THEN 'WARNING'
                 ELSE 'FAIL' END);

    -- DQ Check 3: Balance exceeds loan amount
    SELECT @total = COUNT(*), @failed = ISNULL(SUM(CAST(dq_balance_exceeds_amount AS INT)),0)
    FROM silver.loan_accounts 
    WHERE batch_id = @batch_id;
    INSERT INTO audit.data_quality_log
        (batch_id, layer, table_name, dq_rule_name, dq_rule_description,
         total_rows_checked, rows_passed, rows_failed, threshold_pct, dq_status)
    VALUES (@batch_id, 'SILVER', 'loan_accounts', 'BALANCE_EXCEEDS_PRINCIPAL',
            'current_balance must not exceed 130% of loan_amount',
            @total, @total - @failed, @failed, 0.5,
            CASE WHEN CAST(@failed AS FLOAT)/@total*100 <= 0.5 THEN 'PASS'
                 ELSE 'WARNING' END);

    -- DQ Check 4: Income out of range
    SELECT @total = COUNT(*), @failed = ISNULL(SUM(CAST(dq_income_flag AS INT)),0)
    FROM silver.customer_master WHERE batch_id = @batch_id;
    INSERT INTO audit.data_quality_log
        (batch_id, layer, table_name, dq_rule_name, dq_rule_description,
         total_rows_checked, rows_passed, rows_failed, threshold_pct, dq_status)
    VALUES (@batch_id, 'SILVER', 'customer_master', 'INCOME_OUT_OF_RANGE',
            'monthly_income must be between 0 and 5,000,000',
            @total, @total - @failed, @failed, 2.0,
            CASE WHEN CAST(@failed AS FLOAT)/@total*100 <= 2.0 THEN 'PASS' ELSE 'FAIL' END);

    -- DQ Check 5: Gold fact completeness — fact rows vs repayment source
    DECLARE @rep_count INT = (SELECT COUNT(*) FROM silver.repayments WHERE batch_id = @batch_id);
    DECLARE @fact_count INT = (SELECT COUNT(*) FROM gold.fact_credit_risk WHERE batch_id = @batch_id);
    SET @failed = ABS(@rep_count - @fact_count);
    INSERT INTO audit.data_quality_log
        (batch_id, layer, table_name, dq_rule_name, dq_rule_description,
         total_rows_checked, rows_passed, rows_failed, threshold_pct, dq_status)
    VALUES (@batch_id, 'GOLD', 'fact_credit_risk', 'FACT_ROW_COMPLETENESS',
            'fact_credit_risk row count must match silver.repayments within 1%',
            @rep_count, @fact_count, @failed, 1.0,
            CASE WHEN CAST(@failed AS FLOAT)/NULLIF(@rep_count,0)*100 <= 1.0 THEN 'PASS' ELSE 'FAIL' END);

    SELECT * FROM audit.data_quality_log WHERE batch_id = @batch_id ORDER BY dq_log_id;
    PRINT CONCAT('DQ checks complete for batch: ', @batch_id);
END;
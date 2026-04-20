-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    02_gold_scd2_and_loads.sql
-- Purpose: SCD Type 2 for dim_customer + load procedures for all dimensions
--          and the fact table with full IFRS9 / Basel metric calculations.
-- =============================================================================


-- HELPER: Classify credit score into risk band

CREATE OR ALTER FUNCTION gold.fn_credit_risk_band (@score SMALLINT)
RETURNS VARCHAR(20) AS
BEGIN
    RETURN CASE
        WHEN @score >= 750 THEN 'Very Low'
        WHEN @score >= 680 THEN 'Low'
        WHEN @score >= 600 THEN 'Medium'
        WHEN @score >= 500 THEN 'High'
        ELSE 'Very High'
    END
END;
GO
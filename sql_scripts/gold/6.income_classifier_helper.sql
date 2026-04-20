-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    02_gold_scd2_and_loads.sql
-- Purpose: SCD Type 2 for dim_customer + load procedures for all dimensions
--          and the fact table with full IFRS9 / Basel metric calculations.
-- =============================================================================



-- HELPER: Classify income into bands

CREATE OR ALTER FUNCTION gold.fn_income_band (@income DECIMAL(18,2))
RETURNS VARCHAR(20) AS
BEGIN
    RETURN CASE
        WHEN @income < 10000  THEN 'Low'
        WHEN @income < 30000  THEN 'Medium'
        WHEN @income < 80000  THEN 'High'
        ELSE 'Premium'
    END
END;
GO



-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    01_gold_star_schema.sql
-- Purpose:  star schema —  dimensions.
--          This is the analytical layer consumed by dashboards and reports.
-- =============================================================================

-- DIM: gold.dim_loan
-- ─────────────────────────────────────────────────────────────────────────────
IF OBJECT_ID('gold.dim_loan', 'U') IS NOT NULL DROP TABLE gold.dim_loan;
GO
CREATE TABLE gold.dim_loan (
    loan_key             INT IDENTITY(1,1)   NOT NULL,
    loan_id              VARCHAR(50)         NOT NULL,
    loan_type            VARCHAR(50),
    loan_amount          DECIMAL(38,2),
    interest_rate        DECIMAL(38,2),
    loan_term_months     INT,
    loan_start_date      DATE,
    loan_end_date        DATE,
    loan_status          VARCHAR(50),
    origination_branch   VARCHAR(50),
    collateral_type      VARCHAR(50),

    -- Derived
    loan_size_band       VARCHAR(50),        -- Micro / Small / Medium / Large / Mega
    interest_rate_band   VARCHAR(50),        -- Low / Standard / High / Very High
    is_secured           BIT,               -- 1 = has collateral

    batch_id             INT,
    gold_load_date       DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_dim_loan PRIMARY KEY (loan_key),
    INDEX idx_dim_loan_biz (loan_id)
);
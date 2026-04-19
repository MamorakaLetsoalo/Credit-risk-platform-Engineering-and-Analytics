-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    02_silver_loan_account_table.sql
-- Layer:   Silver (Cleansed + Validated + Standardized)
-- Purpose: Type-cast, deduplicate, standardize, and validate bronze data.
--          Referential integrity enforced here.
-- =============================================================================

-- TABLE: silver.loan_account

IF OBJECT_ID('silver.loan_accounts', 'U') IS NOT NULL DROP TABLE silver.loan_accounts;
GO
CREATE TABLE silver.loan_accounts (
    loan_id              VARCHAR(20)         NOT NULL,
    customer_id          VARCHAR(20)         NOT NULL,
    loan_type            VARCHAR(50)         NOT NULL,
    loan_amount          DECIMAL(18,2)       NOT NULL,
    interest_rate        DECIMAL(5,2)        NOT NULL,
    loan_term_months     INT,
    loan_start_date      DATE                NOT NULL,
    loan_end_date        DATE,
    current_balance      DECIMAL(18,2),
    loan_status          VARCHAR(30)         NOT NULL,
    origination_branch   VARCHAR(20),
    collateral_type      VARCHAR(50),

    -- Derived metrics
    loan_to_value_ratio  AS (CASE WHEN loan_amount > 0
                             THEN CAST(current_balance AS DECIMAL(10,4)) / CAST(loan_amount AS DECIMAL(10,4))
                             ELSE NULL END) PERSISTED,

    -- Data quality flags
    dq_orphan_customer   BIT DEFAULT 0,      -- 1 = no matching customer
    dq_balance_exceeds_amount BIT DEFAULT 0, -- 1 = balance > original amount
    dq_date_mismatch     BIT DEFAULT 0,      -- 1 = start > end date

    -- Lineage
    source_system        VARCHAR(50),
    batch_id             INT,
    silver_load_date     DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_silver_loan PRIMARY KEY (loan_id)
);
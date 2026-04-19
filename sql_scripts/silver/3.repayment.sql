-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    03_silver_repayments_table.sql
-- Layer:   Silver (Cleansed + Validated + Standardized)
-- Purpose: Type-cast, deduplicate, standardize, and validate bronze data.
--          Referential integrity enforced here.
-- =============================================================================

-- TABLE: silver.repayments

IF OBJECT_ID('silver.repayments', 'U') IS NOT NULL DROP TABLE silver.repayments;
GO
CREATE TABLE silver.repayments (
    repayment_id         VARCHAR(20)         NOT NULL,
    loan_id              VARCHAR(20)         NOT NULL,
    payment_date         DATE                NOT NULL,
    due_date             DATE                NOT NULL,
    payment_amount       DECIMAL(18,2)       NOT NULL,
    expected_amount      DECIMAL(18,2),
    days_past_due        INT                 NOT NULL DEFAULT 0,
    repayment_status     VARCHAR(30)         NOT NULL,
    payment_method       VARCHAR(50),

    -- Derived
    is_late              AS (CASE WHEN days_past_due > 0 THEN 1 ELSE 0 END) PERSISTED,
    payment_shortfall    AS (expected_amount - payment_amount) PERSISTED,

    -- DQ
    dq_orphan_loan       BIT DEFAULT 0,
    dq_future_payment    BIT DEFAULT 0,

    -- Lineage
    source_system        VARCHAR(50),
    batch_id             INT,
    silver_load_date     DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_silver_repayment PRIMARY KEY (repayment_id)
);
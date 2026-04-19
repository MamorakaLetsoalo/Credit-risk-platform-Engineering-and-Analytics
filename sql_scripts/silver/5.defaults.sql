-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    05_silver_defaults_table.sql
-- Layer:   Silver (Cleansed + Validated + Standardized)
-- Purpose: Type-cast, deduplicate, standardize, and validate bronze data.
--          Referential integrity enforced here.
-- =============================================================================

-- TABLE: silver.defaults
IF OBJECT_ID('silver.defaults', 'U') IS NOT NULL DROP TABLE silver.defaults;
GO
CREATE TABLE silver.defaults (
    default_id                VARCHAR(20)     NOT NULL,
    loan_id                   VARCHAR(20)     NOT NULL,
    default_date              DATE            NOT NULL,
    default_reason            VARCHAR(100),
    outstanding_at_default    DECIMAL(18,2),
    recovery_amount           DECIMAL(18,2)   DEFAULT 0,
    recovery_status           VARCHAR(50),
    days_to_default           INT,
    npl_classification        VARCHAR(30),
    provision_amount          DECIMAL(18,2),

    -- Derived
    recovery_rate             AS (
        CASE WHEN outstanding_at_default > 0
             THEN CAST(recovery_amount AS DECIMAL(10,4)) / CAST(outstanding_at_default AS DECIMAL(10,4))
             ELSE 0 END
    ) PERSISTED,
--Dq
    dq_orphan_loan            BIT DEFAULT 0,

    --lineage
    source_system             VARCHAR(50),
    batch_id                  INT,
    silver_load_date          DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_silver_default PRIMARY KEY (default_id)
);
-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    01_silver_customer_master_table.sql
-- Layer:   Silver (Cleansed + Validated + Standardized)
-- Purpose: Type-cast, deduplicate, standardize, and validate bronze data.
--          Referential integrity enforced here.
-- =============================================================================

-- TABLE: silver.customer_master
-- ─────────────────────────────────────────────────────────────────────────────
IF OBJECT_ID('silver.customer_master', 'U') IS NOT NULL DROP TABLE silver.customer_master;
GO
CREATE TABLE silver.customer_master (
    customer_id          VARCHAR(20)         NOT NULL,
    customer_name        VARCHAR(200)        NOT NULL,
    id_number            VARCHAR(20),
    date_of_birth        DATE,
    gender               CHAR(1),            -- M / F / U
    employment_status    VARCHAR(50)         NOT NULL,
    monthly_income       DECIMAL(18,2)       NOT NULL,
    marital_status       VARCHAR(30),
    customer_segment     VARCHAR(50)         NOT NULL,
    province             VARCHAR(50),
    email                VARCHAR(200),
    phone                VARCHAR(30),
    credit_score         SMALLINT,
    onboarding_date      DATE                NOT NULL,

    -- Data quality flags
    dq_income_flag       BIT DEFAULT 0,      -- 1 = income out of expected range
    dq_email_flag        BIT DEFAULT 0,      -- 1 = invalid email format
    dq_dob_flag          BIT DEFAULT 0,      -- 1 = DOB in future or impossible

    -- Lineage
    source_system        VARCHAR(50),
    batch_id             INT,
    silver_load_date     DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_silver_customer PRIMARY KEY (customer_id)
);
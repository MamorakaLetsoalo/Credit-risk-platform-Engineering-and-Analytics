-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    02_bronze.loan_accounts.sql
-- Layer:   Bronze (Raw Ingestion)
-- Purpose: Create all raw staging tables with metadata columns for auditing.
--          NO transformations here — data lands exactly as it arrives.
-- Author:  Data Engineer Letsoalo M
-- Version: 1.0
-- =============================================================================

-- TABLE: bronze.loan_accounts
IF OBJECT_ID('bronze.loan_accounts', 'U') IS NOT NULL DROP TABLE bronze.loan_accounts;
GO
CREATE TABLE bronze.loan_accounts (
    loan_id              VARCHAR(50),
    customer_id          VARCHAR(50),
    loan_type            VARCHAR(50),
    loan_amount          VARCHAR(50),
    interest_rate        VARCHAR(50),
    loan_term_months     VARCHAR(50),
    loan_start_date      VARCHAR(50),
    loan_end_date        VARCHAR(50),
    current_balance      VARCHAR(50),
    loan_status          VARCHAR(50),
    origination_branch   VARCHAR(50),
    collateral_type      VARCHAR(50),
    source_system        VARCHAR(50),
    record_created_at    VARCHAR(50),

    -- Pipeline metadata
    load_date            DATETIME2       DEFAULT GETDATE(),
    batch_id             INT,
    source_file          VARCHAR(255),
    row_number_in_file   INT,
    is_duplicate_flag    BIT             DEFAULT 0,
    load_status          VARCHAR(20)     DEFAULT 'LOADED'
);
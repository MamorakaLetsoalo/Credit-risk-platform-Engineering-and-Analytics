-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    03_bronze.repayments.sql
-- Layer:   Bronze (Raw Ingestion)
-- Purpose: Create raw staging table with metadata columns for auditing.
--          NO transformations here — data lands exactly as it arrives.
-- Author:  Data Engineer Letsoalo M
-- Version: 1.0
-- =============================================================================

-- TABLE: bronze.repayments

IF OBJECT_ID('bronze.repayments', 'U') IS NOT NULL DROP TABLE bronze.repayments;
GO
CREATE TABLE bronze.repayments (
    repayment_id         VARCHAR(50),
    loan_id              VARCHAR(50),
    payment_date         VARCHAR(50),
    due_date             VARCHAR(50),
    payment_amount       VARCHAR(50),
    expected_amount      VARCHAR(50),
    days_past_due        VARCHAR(50),
    repayment_status     VARCHAR(50),
    payment_method       VARCHAR(50),
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
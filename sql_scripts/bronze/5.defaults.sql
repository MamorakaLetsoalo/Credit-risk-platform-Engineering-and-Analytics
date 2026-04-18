-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    05_bronze.defaults.sql
-- Layer:   Bronze (Raw Ingestion)
-- Purpose: Create raw staging table with metadata columns for auditing.
--          NO transformations here — data lands exactly as it arrives.
-- Author:  Data Engineer Letsoalo M
-- Version: 1.0
-- =============================================================================

-- TABLE: bronze.defaults
IF OBJECT_ID('bronze.defaults', 'U') IS NOT NULL DROP TABLE bronze.defaults;
GO
CREATE TABLE bronze.defaults (
    default_id                VARCHAR(50),
    loan_id                   VARCHAR(50),
    default_date              VARCHAR(50),
    default_reason            VARCHAR(250),
    outstanding_at_default    VARCHAR(50),
    recovery_amount           VARCHAR(50),
    recovery_status           VARCHAR(50),
    days_to_default           VARCHAR(50),
    npl_classification        VARCHAR(50),
    provision_amount          VARCHAR(50),
    source_system             VARCHAR(50),
    record_created_at         VARCHAR(50),

    -- Pipeline metadata
    load_date                 DATETIME2   DEFAULT GETDATE(),
    batch_id                  INT,
    source_file               VARCHAR(255),
    row_number_in_file        INT,
    is_duplicate_flag         BIT         DEFAULT 0,
    load_status               VARCHAR(20) DEFAULT 'LOADED'
);

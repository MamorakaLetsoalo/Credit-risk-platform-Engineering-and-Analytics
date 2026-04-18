-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    01_bronze.customer_master.sql
-- Layer:   Bronze (Raw Ingestion)
-- Purpose: Create all raw staging tables with metadata columns for auditing.
--          NO transformations here — data lands exactly as it arrives.
-- Author:  Data Engineering Letsoalo M
-- Version: 1.0
-- =============================================================================

-- TABLE: bronze.customer_master

IF OBJECT_ID('bronze.customer_master', 'U') IS NOT NULL DROP TABLE bronze.customer_master;
GO
CREATE TABLE bronze.customer_master (
    -- Source columns
    customer_id          VARCHAR(50),
    customer_name        VARCHAR(50),
    id_number            VARCHAR(50),
    date_of_birth        VARCHAR(50),        -- raw: validated in Silver
    gender               VARCHAR(50),
    employment_status    VARCHAR(50),
    monthly_income       VARCHAR(50),        -- raw VARCHAR: parsed in Silver
    marital_status       VARCHAR(50),
    customer_segment     VARCHAR(50),
    province             VARCHAR(50),
    email                VARCHAR(200),
    phone                VARCHAR(50),
    credit_score         VARCHAR(50),
    onboarding_date      VARCHAR(50),
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
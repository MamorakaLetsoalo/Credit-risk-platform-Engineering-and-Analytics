-- =============================================================================
-- CREDIT RISK PLATFORM | BRONZE LAYER
-- File:    04_bronze.collections.sql
-- Layer:   Bronze (Raw Ingestion)
-- Purpose: Create raw staging table with metadata columns for auditing.
--          NO transformations here — data lands exactly as it arrives.
-- Author:  Data Engineer Letsoalo M
-- Version: 1.0
-- =============================================================================

-- TABLE: bronze.collections

IF OBJECT_ID('bronze.collections', 'U') IS NOT NULL DROP TABLE bronze.collections;
GO
CREATE TABLE bronze.collections (
    collection_id        VARCHAR(50),
    loan_id              VARCHAR(50),
    collector_id         VARCHAR(50),
    action_date          VARCHAR(50),
    action_type          VARCHAR(50),
    outcome              VARCHAR(50),
    promised_amount      VARCHAR(50),
    next_action_date     VARCHAR(50),
    escalation_level     VARCHAR(50),
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
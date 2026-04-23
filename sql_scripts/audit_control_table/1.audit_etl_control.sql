-- =============================================================================
-- CREDIT RISK PLATFORM | AUDIT & CONTROL LAYER
-- File:    01_audit_control_tables.sql
-- Purpose: ETL control table, audit log, data quality summary, error log.
--          These tables power the observability and operational monitoring
--          of the entire pipeline.
-- =============================================================================

USE CreditRiskDB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA audit');
GO

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: audit.etl_control
-- Master control table — one row per process per run.
-- SSIS reads last_successful_load for incremental logic.
-- ─────────────────────────────────────────────────────────────────────────────
IF OBJECT_ID('audit.etl_control', 'U') IS NOT NULL DROP TABLE audit.etl_control;
GO
CREATE TABLE audit.etl_control (
    control_id              INT IDENTITY(1,1)   NOT NULL,
    process_name            VARCHAR(100)        NOT NULL,   -- e.g. 'BRONZE_REPAYMENTS_LOAD'
    layer                   VARCHAR(20),                    -- BRONZE / SILVER / GOLD
    batch_id                INT                 NOT NULL,
    run_date                DATE                NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    start_time              DATETIME2           NOT NULL DEFAULT GETDATE(),
    end_time                DATETIME2,
    status                  VARCHAR(20)         NOT NULL DEFAULT 'RUNNING',
                                                            -- RUNNING / SUCCESS / FAILED / SKIPPED
    rows_read               INT                 DEFAULT 0,
    rows_inserted           INT                 DEFAULT 0,
    rows_updated            INT                 DEFAULT 0,
    rows_rejected           INT                 DEFAULT 0,
    last_successful_load    DATETIME2,
    source_file             VARCHAR(500),
    error_message           VARCHAR(MAX),
    duration_seconds        AS (
        CASE WHEN end_time IS NOT NULL
             THEN DATEDIFF(SECOND, start_time, end_time)
             ELSE NULL END
    ),

    CONSTRAINT pk_etl_control PRIMARY KEY (control_id)
);
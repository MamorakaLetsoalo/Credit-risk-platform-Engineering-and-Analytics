-- =============================================================================
-- CREDIT RISK PLATFORM | AUDIT & CONTROL LAYER
-- File:    02_audit_control_tables.sql
-- Purpose: ETL control table, audit log, data quality summary, error log.
--          These tables power the observability and operational monitoring
--          of the entire pipeline.
-- =============================================================================

-- TABLE: audit.etl_batch
-- One row per full pipeline run (across all layers/processes)
IF OBJECT_ID('audit.etl_batch', 'U') IS NOT NULL DROP TABLE audit.etl_batch;
GO
CREATE TABLE audit.etl_batch (
    batch_id                INT IDENTITY(1,1)   NOT NULL,
    batch_start_time        DATETIME2           NOT NULL DEFAULT GETDATE(),
    batch_end_time          DATETIME2,
    batch_status            VARCHAR(20)         DEFAULT 'RUNNING',
    triggered_by            VARCHAR(100),       -- SSIS_SCHEDULE / MANUAL / CI_CD
    environment             VARCHAR(20),        -- DEV / UAT / PROD
    git_commit_hash         VARCHAR(50),
    pipeline_version        VARCHAR(20),
    total_rows_processed    INT                 DEFAULT 0,
    total_errors            INT                 DEFAULT 0,
    notes                   VARCHAR(MAX),

    CONSTRAINT pk_etl_batch PRIMARY KEY (batch_id)
);
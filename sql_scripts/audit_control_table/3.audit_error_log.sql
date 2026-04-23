-- =============================================================================
-- CREDIT RISK PLATFORM | AUDIT & CONTROL LAYER
-- File:    03_audit_control_tables.sql
-- Purpose: ETL control table, audit log, data quality summary, error log.
--          These tables power the observability and operational monitoring
--          of the entire pipeline.
-- =============================================================================

-- TABLE: audit.data_quality_log
-- One row per DQ rule check per batch. Powers DQ dashboard.

IF OBJECT_ID('audit.data_quality_log', 'U') IS NOT NULL DROP TABLE audit.data_quality_log;
GO
CREATE TABLE audit.data_quality_log (
    dq_log_id               INT IDENTITY(1,1)   NOT NULL,
    batch_id                INT                 NOT NULL,
    layer                   VARCHAR(20)         NOT NULL,
    table_name              VARCHAR(100)        NOT NULL,
    dq_rule_name            VARCHAR(100)        NOT NULL,
    dq_rule_description     VARCHAR(500),
    total_rows_checked      INT,
    rows_passed             INT,
    rows_failed             INT,
    failure_rate_pct        AS (
        CASE WHEN total_rows_checked > 0
             THEN CAST(rows_failed AS FLOAT) / total_rows_checked * 100
             ELSE 0 END
    ),
    threshold_pct           DECIMAL(5,2),       -- max acceptable failure %
    dq_status               VARCHAR(20),        -- PASS / FAIL / WARNING
    checked_at              DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_dq_log PRIMARY KEY (dq_log_id)
);
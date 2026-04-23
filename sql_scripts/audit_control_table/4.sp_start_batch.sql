-- =============================================================================
-- CREDIT RISK PLATFORM | AUDIT & CONTROL LAYER
-- File:    04_audit_control_tables.sql
-- Purpose: ETL control table, audit log, data quality summary, error log.
--          These tables power the observability and operational monitoring
--          of the entire pipeline.
-- =============================================================================

-- STORED PROCEDURES: Audit Helpers (called from SSIS tasks)


-- Start a new batch
CREATE OR ALTER PROCEDURE audit.usp_start_batch
    @triggered_by       VARCHAR(100) = 'SSIS_SCHEDULE',
    @environment        VARCHAR(20)  = 'PROD',
    @pipeline_version   VARCHAR(20)  = '1.0',
    @git_commit_hash    VARCHAR(50)  = NULL,
    @batch_id           INT OUTPUT
AS
BEGIN
    INSERT INTO audit.etl_batch (triggered_by, environment, pipeline_version, git_commit_hash)
    VALUES (@triggered_by, @environment, @pipeline_version, @git_commit_hash);
    SET @batch_id = SCOPE_IDENTITY();
    PRINT CONCAT('Batch started: ', @batch_id);
END;
-- =============================================================================
-- CREDIT RISK PLATFORM | AUDIT & CONTROL LAYER
-- File:    06_audit_control_tables.sql
-- Purpose: ETL control table, audit log, data quality summary, error log.
--          These tables power the observability and operational monitoring
--          of the entire pipeline.
-- =============================================================================


--STORED PROCEDURES: Audit Helpers (called from SSIS tasks)
-- Log a process step
CREATE OR ALTER PROCEDURE audit.usp_log_process
    @process_name       VARCHAR(100),
    @layer              VARCHAR(20),
    @batch_id           INT,
    @status             VARCHAR(20),
    @rows_read          INT = 0,
    @rows_inserted      INT = 0,
    @rows_updated       INT = 0,
    @rows_rejected      INT = 0,
    @last_load          DATETIME2 = NULL,
    @source_file        VARCHAR(500) = NULL,
    @error_message      VARCHAR(MAX) = NULL
AS
BEGIN
    MERGE audit.etl_control AS tgt
    USING (SELECT @process_name AS p, @batch_id AS b) AS src
    ON tgt.process_name = src.p AND tgt.batch_id = src.b
    WHEN MATCHED THEN
        UPDATE SET
            status = @status, end_time = GETDATE(),
            rows_read = @rows_read, rows_inserted = @rows_inserted,
            rows_updated = @rows_updated, rows_rejected = @rows_rejected,
            last_successful_load = CASE WHEN @status='SUCCESS' THEN GETDATE() ELSE last_successful_load END,
            error_message = @error_message
    WHEN NOT MATCHED THEN
        INSERT (process_name, layer, batch_id, status, rows_read, rows_inserted,
                rows_updated, rows_rejected, last_successful_load, source_file, error_message)
        VALUES (@process_name, @layer, @batch_id, @status, @rows_read, @rows_inserted,
                @rows_updated, @rows_rejected, @last_load, @source_file, @error_message);
END;

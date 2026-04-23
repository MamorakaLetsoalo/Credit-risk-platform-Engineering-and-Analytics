-- =============================================================================
-- CREDIT RISK PLATFORM | AUDIT & CONTROL LAYER
-- File:    06_audit_control_tables.sql
-- Purpose: ETL control table, audit log, data quality summary, error log.
--          These tables power the observability and operational monitoring
--          of the entire pipeline.
-- =============================================================================


--STORED PROCEDURES: Audit Helpers (called from SSIS tasks)
-- Close a batch
CREATE OR ALTER PROCEDURE audit.usp_close_batch
    @batch_id       INT,
    @batch_status   VARCHAR(20),
    @total_rows     INT,
    @total_errors   INT
AS
BEGIN
    UPDATE audit.etl_batch
    SET    batch_end_time         = GETDATE(),
           batch_status           = @batch_status,
           total_rows_processed   = @total_rows,
           total_errors           = @total_errors
    WHERE  batch_id = @batch_id;
END;
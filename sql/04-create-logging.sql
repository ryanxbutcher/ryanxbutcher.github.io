-- ============================================================================
-- EMS Data Warehouse - Logging & Audit Tables
-- Author: J. Ryan Butcher
-- Purpose: ETL execution tracking and error capture
-- ============================================================================

-- Create logging schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    EXEC('CREATE SCHEMA log');
GO

-- ============================================================================
-- ETL_RUN_LOG: Master log - one row per ETL execution
-- ============================================================================
CREATE TABLE log.ETL_RUN_LOG (
    run_id                  BIGINT IDENTITY(1,1) PRIMARY KEY
    ,run_start_dt           DATETIME2 NOT NULL DEFAULT GETDATE()
    ,run_end_dt             DATETIME2
    ,status                 VARCHAR(20) NOT NULL DEFAULT 'RUNNING'  -- RUNNING, SUCCESS, FAILED
    ,source_file            VARCHAR(500)
    ,source_row_count       BIGINT
    ,environment            VARCHAR(20)                             -- dev, test, prod
    ,load_type              VARCHAR(20)                             -- full, incremental
    ,initiated_by           VARCHAR(100)
    ,error_message          NVARCHAR(MAX)
    ,CONSTRAINT CHK_ETL_RUN_STATUS CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'))
);
GO

CREATE NONCLUSTERED INDEX IX_ETL_RUN_LOG_STATUS
    ON log.ETL_RUN_LOG (status, run_start_dt DESC);
GO

-- ============================================================================
-- ETL_STEP_LOG: Detail log - one row per ETL step
-- ============================================================================
CREATE TABLE log.ETL_STEP_LOG (
    step_log_id             BIGINT IDENTITY(1,1) PRIMARY KEY
    ,run_id                 BIGINT NOT NULL
    ,step_name              VARCHAR(100) NOT NULL
    ,step_order             INT
    ,step_start_dt          DATETIME2 NOT NULL DEFAULT GETDATE()
    ,step_end_dt            DATETIME2
    ,status                 VARCHAR(20) NOT NULL DEFAULT 'RUNNING'
    ,rows_read              BIGINT DEFAULT 0
    ,rows_inserted          BIGINT DEFAULT 0
    ,rows_updated           BIGINT DEFAULT 0
    ,rows_rejected          BIGINT DEFAULT 0
    ,rows_deleted           BIGINT DEFAULT 0
    ,error_message          NVARCHAR(MAX)
    ,CONSTRAINT FK_ETL_STEP_RUN FOREIGN KEY (run_id) REFERENCES log.ETL_RUN_LOG(run_id)
);
GO

CREATE NONCLUSTERED INDEX IX_ETL_STEP_LOG_RUN
    ON log.ETL_STEP_LOG (run_id, step_order);
GO

-- ============================================================================
-- ETL_ERROR_LOG: Detailed error capture with source data
-- ============================================================================
CREATE TABLE log.ETL_ERROR_LOG (
    error_id                BIGINT IDENTITY(1,1) PRIMARY KEY
    ,run_id                 BIGINT NOT NULL
    ,step_name              VARCHAR(100)
    ,error_dt               DATETIME2 NOT NULL DEFAULT GETDATE()
    ,source_row_num         INT
    ,error_type             VARCHAR(100)                            -- VALIDATION, PARSE, LOOKUP, etc.
    ,error_code             VARCHAR(50)
    ,error_message          NVARCHAR(MAX)
    ,column_name            VARCHAR(100)
    ,column_value           NVARCHAR(500)
    ,source_data            NVARCHAR(MAX)                           -- Full row for debugging
    ,CONSTRAINT FK_ETL_ERROR_RUN FOREIGN KEY (run_id) REFERENCES log.ETL_RUN_LOG(run_id)
);
GO

CREATE NONCLUSTERED INDEX IX_ETL_ERROR_LOG_RUN
    ON log.ETL_ERROR_LOG (run_id, error_type);
GO

-- ============================================================================
-- ETL_METRICS_LOG: Performance metrics tracking
-- ============================================================================
CREATE TABLE log.ETL_METRICS_LOG (
    metric_id               BIGINT IDENTITY(1,1) PRIMARY KEY
    ,run_id                 BIGINT NOT NULL
    ,step_name              VARCHAR(100)
    ,metric_name            VARCHAR(100) NOT NULL
    ,metric_value           DECIMAL(18,4)
    ,metric_unit            VARCHAR(50)                             -- rows, seconds, MB, etc.
    ,recorded_dt            DATETIME2 NOT NULL DEFAULT GETDATE()
    ,CONSTRAINT FK_ETL_METRICS_RUN FOREIGN KEY (run_id) REFERENCES log.ETL_RUN_LOG(run_id)
);
GO

-- ============================================================================
-- Stored Procedures for Logging
-- ============================================================================

-- Start a new ETL run
CREATE OR ALTER PROCEDURE log.sp_StartRun
    @source_file VARCHAR(500)
    ,@environment VARCHAR(20)
    ,@load_type VARCHAR(20)
    ,@initiated_by VARCHAR(100) = NULL
    ,@run_id BIGINT OUTPUT
AS
BEGIN
    INSERT INTO log.ETL_RUN_LOG (source_file, environment, load_type, initiated_by)
    VALUES (@source_file, @environment, @load_type, COALESCE(@initiated_by, SYSTEM_USER));

    SET @run_id = SCOPE_IDENTITY();
END;
GO

-- End an ETL run
CREATE OR ALTER PROCEDURE log.sp_EndRun
    @run_id BIGINT
    ,@status VARCHAR(20)
    ,@source_row_count BIGINT = NULL
    ,@error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    UPDATE log.ETL_RUN_LOG
    SET run_end_dt = GETDATE()
        ,status = @status
        ,source_row_count = @source_row_count
        ,error_message = @error_message
    WHERE run_id = @run_id;
END;
GO

-- Log a step
CREATE OR ALTER PROCEDURE log.sp_LogStep
    @run_id BIGINT
    ,@step_name VARCHAR(100)
    ,@step_order INT
    ,@status VARCHAR(20)
    ,@rows_read BIGINT = 0
    ,@rows_inserted BIGINT = 0
    ,@rows_updated BIGINT = 0
    ,@rows_rejected BIGINT = 0
    ,@error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    INSERT INTO log.ETL_STEP_LOG (
        run_id, step_name, step_order, step_end_dt, status
        ,rows_read, rows_inserted, rows_updated, rows_rejected, error_message
    )
    VALUES (
        @run_id, @step_name, @step_order, GETDATE(), @status
        ,@rows_read, @rows_inserted, @rows_updated, @rows_rejected, @error_message
    );
END;
GO

-- Log an error
CREATE OR ALTER PROCEDURE log.sp_LogError
    @run_id BIGINT
    ,@step_name VARCHAR(100)
    ,@source_row_num INT
    ,@error_type VARCHAR(100)
    ,@error_message NVARCHAR(MAX)
    ,@column_name VARCHAR(100) = NULL
    ,@column_value NVARCHAR(500) = NULL
    ,@source_data NVARCHAR(MAX) = NULL
AS
BEGIN
    INSERT INTO log.ETL_ERROR_LOG (
        run_id, step_name, source_row_num, error_type
        ,error_message, column_name, column_value, source_data
    )
    VALUES (
        @run_id, @step_name, @source_row_num, @error_type
        ,@error_message, @column_name, @column_value, @source_data
    );
END;
GO

PRINT '✅ Logging tables and procedures created successfully';
GO

-- ============================================================
-- NYC Taxi Pipeline — Watermark & Audit Setup (PRODUCTION)
-- Run this in Azure SQL Database
-- ============================================================

-- ================================
-- 1. WATERMARK TABLE
-- ================================
IF OBJECT_ID('pipeline_watermark', 'U') IS NULL
BEGIN
    CREATE TABLE pipeline_watermark (
        watermark_id          INT IDENTITY(1,1) PRIMARY KEY,
        source_table_name     VARCHAR(100)  NOT NULL,
        source_schema         VARCHAR(50)   NOT NULL,
        watermark_column      VARCHAR(100)  NOT NULL,
        last_watermark_value  DATETIME2     NOT NULL,
        last_run_status       VARCHAR(20)   NOT NULL DEFAULT 'SUCCESS',
        last_run_rows_copied  INT           DEFAULT 0,
        last_run_start_time   DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        last_run_end_time     DATETIME2,
        pipeline_run_id       VARCHAR(100),
        created_at            DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        updated_at            DATETIME2     NOT NULL DEFAULT GETUTCDATE()
    );

    -- Ensure one row per source table
    ALTER TABLE pipeline_watermark
    ADD CONSTRAINT uq_source_table UNIQUE (source_table_name);

    -- Performance index
    CREATE INDEX idx_watermark_table
    ON pipeline_watermark (source_table_name);
END
GO

-- ================================
-- 2. AUDIT LOG TABLE
-- ================================
IF OBJECT_ID('pipeline_audit_log', 'U') IS NULL
BEGIN
    CREATE TABLE pipeline_audit_log (
        log_id            INT IDENTITY(1,1) PRIMARY KEY,
        pipeline_name     VARCHAR(100)  NOT NULL,
        pipeline_run_id   VARCHAR(100)  NOT NULL,
        source_table      VARCHAR(100)  NOT NULL,
        watermark_start   DATETIME2     NOT NULL,
        watermark_end     DATETIME2     NOT NULL,
        rows_copied       INT           DEFAULT 0,
        status            VARCHAR(20)   NOT NULL,
        error_message     VARCHAR(MAX),
        bronze_path       VARCHAR(500),
        run_start_time    DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        run_end_time      DATETIME2,
        duration_seconds  AS DATEDIFF(SECOND, run_start_time, run_end_time)
    );
END
GO

-- ================================
-- 3. SEED INITIAL WATERMARK
-- ================================
IF NOT EXISTS (
    SELECT 1 FROM pipeline_watermark WHERE source_table_name = 'taxi_trips'
)
BEGIN
    INSERT INTO pipeline_watermark (
        source_table_name,
        source_schema,
        watermark_column,
        last_watermark_value,
        last_run_status
    )
    VALUES (
        'taxi_trips',
        'nyc_taxi_source',
        'updated_at',
        '2024-01-01 00:00:00',
        'INIT'
    );
END
GO

-- ================================
-- 4. STORED PROCEDURE
-- ================================
CREATE OR ALTER PROCEDURE usp_update_watermark
    @source_table        VARCHAR(100),
    @new_watermark_value DATETIME2,
    @pipeline_run_id     VARCHAR(100),
    @rows_copied         INT
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        DECLARE @old_watermark DATETIME2;

        -- Get previous watermark
        SELECT @old_watermark = last_watermark_value
        FROM pipeline_watermark
        WHERE source_table_name = @source_table;

        -- Update watermark
        UPDATE pipeline_watermark
        SET
            last_watermark_value = @new_watermark_value,
            last_run_status      = 'SUCCESS',
            last_run_rows_copied = @rows_copied,
            last_run_end_time    = GETUTCDATE(),
            pipeline_run_id      = @pipeline_run_id,
            updated_at           = GETUTCDATE()
        WHERE source_table_name = @source_table;

        -- Insert audit log
        INSERT INTO pipeline_audit_log (
            pipeline_name,
            pipeline_run_id,
            source_table,
            watermark_start,
            watermark_end,
            rows_copied,
            status,
            run_end_time
        )
        VALUES (
            'pl_incremental_mysql_to_bronze',
            @pipeline_run_id,
            @source_table,
            @old_watermark,
            @new_watermark_value,
            @rows_copied,
            'SUCCESS',
            GETUTCDATE()
        );

    END TRY
    BEGIN CATCH

        INSERT INTO pipeline_audit_log (
            pipeline_name,
            pipeline_run_id,
            source_table,
            watermark_start,
            watermark_end,
            status,
            error_message,
            run_end_time
        )
        VALUES (
            'pl_incremental_mysql_to_bronze',
            @pipeline_run_id,
            @source_table,
            NULL,
            NULL,
            'FAILED',
            ERROR_MESSAGE(),
            GETUTCDATE()
        );

        THROW;

    END CATCH
END
GO

-- ================================
-- 5. VERIFY SETUP
-- ================================
SELECT * FROM pipeline_watermark;

SELECT name AS procedure_name
FROM sys.procedures
WHERE name = 'usp_update_watermark';
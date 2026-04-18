IF OBJECT_ID('ingestion_watermark', 'U') IS NULL
BEGIN
CREATE TABLE ingestion_watermark (
    watermark_id           INT IDENTITY(1,1) PRIMARY KEY,

    source_system          VARCHAR(50)   NOT NULL,
    source_schema          VARCHAR(50),
    source_table_name      VARCHAR(100)  NOT NULL,

    watermark_column       VARCHAR(100)  NOT NULL,
    last_watermark_value   DATETIME2     NOT NULL,

    last_run_status        VARCHAR(20)   DEFAULT 'SUCCESS',
    last_run_rows_processed INT          DEFAULT 0,   -- 🔥 renamed

    last_run_start_time    DATETIME2     DEFAULT GETUTCDATE(),
    last_run_end_time      DATETIME2,

    pipeline_name          VARCHAR(100),
    pipeline_run_id        VARCHAR(100),

    data_path              VARCHAR(500),  -- 🔥 generic

    created_at             DATETIME2     DEFAULT GETUTCDATE(),
    updated_at             DATETIME2     DEFAULT GETUTCDATE(),

    CONSTRAINT uq_ingestion UNIQUE (source_system, source_table_name)
);

CREATE INDEX idx_ingestion 
ON ingestion_watermark (source_table_name);
END
GO

IF OBJECT_ID('pipeline_audit_log', 'U') IS NULL
BEGIN
CREATE TABLE pipeline_audit_log (
    log_id             INT IDENTITY(1,1) PRIMARY KEY,

    pipeline_name      VARCHAR(100),
    pipeline_run_id    VARCHAR(100),

    layer_name         VARCHAR(20),

    source_table       VARCHAR(100),
    target_table       VARCHAR(100),

    watermark_start    DATETIME2,
    watermark_end      DATETIME2,

    rows_processed     INT DEFAULT 0,

    status             VARCHAR(20),
    error_message      VARCHAR(MAX),

    data_path          VARCHAR(500),

    run_start_time     DATETIME2 DEFAULT GETUTCDATE(),
    run_end_time       DATETIME2
);
END
GO
---seeds bronze ingestion watermark
IF NOT EXISTS (
    SELECT 1 FROM ingestion_watermark 
    WHERE source_system = 'mysql' 
    AND source_table_name = 'taxi_trips'
)
BEGIN
    INSERT INTO ingestion_watermark (
        source_system,
        source_schema,
        source_table_name,
        watermark_column,
        last_watermark_value,
        last_run_status
    )
    VALUES (
        'mysql',
        'nyc_taxi_source',
        'taxi_trips',
        'updated_at',
        '2024-01-01 00:00:00',
        'INIT'
    );
END


IF OBJECT_ID('ingestion_validation_config', 'U') IS NULL
BEGIN
    CREATE TABLE ingestion_validation_config (
        source_table_name    VARCHAR(100),
        min_expected_rows    INT,
        allow_zero_load      BIT DEFAULT 0
    );
END
GO



--- seeding  ingestion validation config for taxi_trips, can be extended to other tables and used in pipelines to validate data quality after ingestion
IF NOT EXISTS (
    SELECT 1 FROM ingestion_validation_config
    WHERE source_table_name = 'taxi_trips'
)
BEGIN
    INSERT INTO ingestion_validation_config
    VALUES ('taxi_trips', 1, 0);
END


IF OBJECT_ID('transformation_watermark', 'U') IS NULL
BEGIN
CREATE TABLE transformation_watermark (
    watermark_id            INT IDENTITY(1,1) PRIMARY KEY,

    layer_name              VARCHAR(20)   NOT NULL,   -- silver / gold
    source_layer            VARCHAR(20)   NOT NULL,   -- bronze / silver

    source_schema           VARCHAR(50),
    source_table_name       VARCHAR(100)  NOT NULL,

    target_schema           VARCHAR(50),
    target_table_name       VARCHAR(100)  NOT NULL,

    watermark_column        VARCHAR(100)  NOT NULL,
    last_watermark_value    DATETIME2     NULL,

    last_run_status         VARCHAR(20)   DEFAULT 'SUCCESS',
    last_run_rows_processed INT           DEFAULT 0,

    last_run_start_time     DATETIME2     DEFAULT GETUTCDATE(),
    last_run_end_time       DATETIME2,

    job_name                VARCHAR(100),
    job_run_id              VARCHAR(100),

    data_path               VARCHAR(500),   -- 🔥 NEW (important)

    created_at              DATETIME2     DEFAULT GETUTCDATE(),
    updated_at              DATETIME2     DEFAULT GETUTCDATE(),

    CONSTRAINT uq_transformation 
    UNIQUE (layer_name, source_table_name, target_table_name)
);

CREATE INDEX idx_transformation_lookup 
ON transformation_watermark (layer_name, target_table_name);
END
GO

--seeds silver transformation_watermark

IF NOT EXISTS (
    SELECT 1 FROM transformation_watermark
    WHERE layer_name = 'silver'
    AND target_table_name = 'taxi_trips'
)
BEGIN
    INSERT INTO transformation_watermark (
        layer_name,
        source_layer,
        source_table_name,
        target_table_name,
        watermark_column,
        last_watermark_value,
        last_run_status
    )
    VALUES (
        'silver',
        'bronze',
        'taxi_trips',
        'taxi_trips',
        'updated_at',
        '1900-01-01 00:00:00',
        'INIT'
    );
END


--Seeding gold transformation watermark
IF NOT EXISTS (
    SELECT 1 FROM transformation_watermark
    WHERE layer_name = 'gold'
    AND target_table_name = 'taxi_trips'
)
BEGIN
    INSERT INTO transformation_watermark (
        layer_name,
        source_layer,
        source_table_name,
        target_table_name,
        watermark_column,
        last_watermark_value,
        last_run_status
    )
    VALUES (
        'gold',
        'silver',
        'taxi_trips',
        'taxi_trips',
        'updated_at',
        '1900-01-01 00:00:00',
        'INIT'
    );
END



-- =============================================
-- usp_update_ingestion_watermark
-- ONLY updates watermark table, NO audit log
-- =============================================
CREATE OR ALTER PROCEDURE usp_update_ingestion_watermark
(
    @source_system        VARCHAR(50),
    @source_table         VARCHAR(100),
    @layer_name           VARCHAR(20),
    @new_watermark_value  DATETIME2,
    @pipeline_name        VARCHAR(100),
    @pipeline_run_id      VARCHAR(100),
    @rows_processed       INT,
    @data_path            VARCHAR(500)
)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        DECLARE @old_watermark DATETIME2;

        SELECT @old_watermark = last_watermark_value
        FROM ingestion_watermark
        WHERE source_system = @source_system
          AND source_table_name = @source_table;

        IF @old_watermark IS NULL
        BEGIN
            INSERT INTO ingestion_watermark (
                source_system,
                source_table_name,
                watermark_column,
                last_watermark_value,
                last_run_status,
                last_run_rows_processed,
                last_run_end_time,
                pipeline_name,
                pipeline_run_id,
                data_path,
                created_at,
                updated_at
            )
            VALUES (
                @source_system,
                @source_table,
                'updated_at',
                @new_watermark_value,
                'SUCCESS',
                @rows_processed,
                GETUTCDATE(),
                @pipeline_name,
                @pipeline_run_id,
                @data_path,
                GETUTCDATE(),
                GETUTCDATE()
            );
        END
        ELSE
        BEGIN
            UPDATE ingestion_watermark
            SET
                last_watermark_value    = @new_watermark_value,
                last_run_status         = 'SUCCESS',
                last_run_rows_processed = @rows_processed,
                last_run_end_time       = GETUTCDATE(),
                pipeline_name           = @pipeline_name,
                pipeline_run_id         = @pipeline_run_id,
                data_path               = @data_path,
                updated_at              = GETUTCDATE()
            WHERE source_system = @source_system
              AND source_table_name = @source_table;
        END

    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
GO


-- =============================================
-- usp_update_transformation_watermark
-- ONLY updates watermark table, NO audit log
-- =============================================
CREATE OR ALTER PROCEDURE usp_update_transformation_watermark
(
    @layer_name           VARCHAR(20),
    @source_table         VARCHAR(100),
    @target_table         VARCHAR(100),
    @new_watermark_value  DATETIME2,
    @rows_processed       INT,
    @pipeline_name        VARCHAR(100),
    @pipeline_run_id      VARCHAR(100),
    @data_path            VARCHAR(500)
)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        DECLARE @old_watermark DATETIME2;

        SELECT @old_watermark = last_watermark_value
        FROM transformation_watermark
        WHERE layer_name = @layer_name
          AND target_table_name = @target_table;

        IF @old_watermark IS NULL
        BEGIN
            INSERT INTO transformation_watermark (
                layer_name,
                source_layer,
                source_table_name,
                target_table_name,
                watermark_column,
                last_watermark_value,
                last_run_status,
                last_run_rows_processed,
                last_run_end_time,
                job_name,
                job_run_id,
                data_path,
                updated_at
            )
            VALUES (
                @layer_name,
                'bronze',
                @source_table,
                @target_table,
                'updated_at',
                @new_watermark_value,
                'SUCCESS',
                @rows_processed,
                GETUTCDATE(),
                @pipeline_name,
                @pipeline_run_id,
                @data_path,
                GETUTCDATE()
            );
        END
        ELSE
        BEGIN
            UPDATE transformation_watermark
            SET
                last_watermark_value    = @new_watermark_value,
                last_run_status         = 'SUCCESS',
                last_run_rows_processed = @rows_processed,
                last_run_end_time       = GETUTCDATE(),
                job_name                = @pipeline_name,
                job_run_id              = @pipeline_run_id,
                data_path               = @data_path,
                updated_at              = GETUTCDATE()
            WHERE layer_name = @layer_name
              AND target_table_name = @target_table;
        END

    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
GO


-- =============================================
-- usp_log_pipeline_execution
-- ONLY place that writes to audit log
-- Handles SUCCESS, NO_DATA, FAILED
-- =============================================
CREATE OR ALTER PROCEDURE usp_log_pipeline_execution
(
    @pipeline_name     VARCHAR(100),
    @pipeline_run_id   VARCHAR(100),
    @layer_name        VARCHAR(20),
    @source_table      VARCHAR(100),
    @target_table      VARCHAR(100)  = NULL,
    @status            VARCHAR(20),
    @rows_processed    INT           = 0,
    @watermark_start   DATETIME2     = NULL,
    @watermark_end     DATETIME2     = NULL,
    @error_message     VARCHAR(MAX)  = NULL,
    @data_path         VARCHAR(500)  = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO pipeline_audit_log (
        pipeline_name,
        pipeline_run_id,
        layer_name,
        source_table,
        target_table,
        watermark_start,
        watermark_end,
        rows_processed,
        status,
        error_message,
        data_path,
        run_end_time
    )
    VALUES (
        @pipeline_name,
        @pipeline_run_id,
        @layer_name,
        @source_table,
        @target_table,
        @watermark_start,
        @watermark_end,
        @rows_processed,
        @status,
        @error_message,
        @data_path,
        GETUTCDATE()
    );
END
GO
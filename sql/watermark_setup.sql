IF OBJECT_ID('ingestion_watermark', 'U') IS NULL
BEGIN
CREATE TABLE ingestion_watermark (
    watermark_id           INT IDENTITY(1,1) PRIMARY KEY,

    source_system          VARCHAR(50)   NOT NULL,   -- mysql / api
    source_schema          VARCHAR(50),
    source_table_name      VARCHAR(100)  NOT NULL,

    watermark_column       VARCHAR(100)  NOT NULL,   -- updated_at
    last_watermark_value   DATETIME2     NOT NULL,

    last_run_status        VARCHAR(20)   DEFAULT 'SUCCESS',
    last_run_rows_copied   INT           DEFAULT 0,

    last_run_start_time    DATETIME2     DEFAULT GETUTCDATE(),
    last_run_end_time      DATETIME2,

    pipeline_name          VARCHAR(100),
    pipeline_run_id        VARCHAR(100),

    bronze_path            VARCHAR(500),

    created_at             DATETIME2     DEFAULT GETUTCDATE(),
    updated_at             DATETIME2     DEFAULT GETUTCDATE(),

    CONSTRAINT uq_ingestion UNIQUE (source_system, source_table_name)
);

CREATE INDEX idx_ingestion 
ON ingestion_watermark (source_table_name);
END
GO





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
    last_watermark_value    DATETIME2     NOT NULL,

    last_run_status         VARCHAR(20)   DEFAULT 'SUCCESS',
    last_run_rows_processed INT           DEFAULT 0,

    last_run_start_time     DATETIME2     DEFAULT GETUTCDATE(),
    last_run_end_time       DATETIME2,

    job_name                VARCHAR(100),
    job_run_id              VARCHAR(100),

    created_at              DATETIME2     DEFAULT GETUTCDATE(),
    updated_at              DATETIME2     DEFAULT GETUTCDATE(),

    CONSTRAINT uq_transformation 
    UNIQUE (layer_name, source_table_name, target_table_name)
);

-- Better index for lookup
CREATE INDEX idx_transformation_lookup 
ON transformation_watermark (layer_name, target_table_name);
END
GO











CREATE TABLE pipeline_audit_log (
    log_id             INT IDENTITY(1,1) PRIMARY KEY,

    pipeline_name      VARCHAR(100),
    pipeline_run_id    VARCHAR(100),

    layer_name         VARCHAR(20),   -- bronze / silver / gold

    source_table       VARCHAR(100),
    target_table       VARCHAR(100),

    watermark_start    DATETIME2,
    watermark_end      DATETIME2,

    rows_processed     INT DEFAULT 0,

    status             VARCHAR(20),
    error_message      VARCHAR(MAX),

    data_path          VARCHAR(500),

    run_start_time     DATETIME2 DEFAULT GETUTCDATE(),
    run_end_time       DATETIME2,

    duration_seconds   AS DATEDIFF(SECOND, run_start_time, run_end_time)
);


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













CREATE OR ALTER PROCEDURE usp_update_ingestion_watermark
    @source_system        VARCHAR(50),
    @source_table         VARCHAR(100),
    @new_watermark_value  DATETIME2,
    @pipeline_name        VARCHAR(100),
    @pipeline_run_id      VARCHAR(100),
    @rows_copied          INT,
    @bronze_path          VARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        DECLARE @old_watermark DATETIME2;

        -- Get previous watermark
        SELECT @old_watermark = last_watermark_value
        FROM ingestion_watermark
        WHERE source_system = @source_system
          AND source_table_name = @source_table;

        -- If not exists → insert (FIRST RUN)
        IF @old_watermark IS NULL
        BEGIN
            INSERT INTO ingestion_watermark (
                source_system,
                source_table_name,
                watermark_column,
                last_watermark_value,
                last_run_status,
                pipeline_name,
                pipeline_run_id,
                bronze_path,
                created_at,
                updated_at
            )
            VALUES (
                @source_system,
                @source_table,
                'updated_at',
                @new_watermark_value,
                'SUCCESS',
                @pipeline_name,
                @pipeline_run_id,
                @bronze_path,
                GETUTCDATE(),
                GETUTCDATE()
            );
        END
        ELSE
        BEGIN
            -- Update existing record
            UPDATE ingestion_watermark
            SET
                last_watermark_value = @new_watermark_value,
                last_run_status      = 'SUCCESS',
                last_run_rows_copied = @rows_copied,
                last_run_end_time    = GETUTCDATE(),
                pipeline_name        = @pipeline_name,
                pipeline_run_id      = @pipeline_run_id,
                bronze_path          = @bronze_path,
                updated_at           = GETUTCDATE()
            WHERE source_system = @source_system
              AND source_table_name = @source_table;
        END

        -- ✅ AUDIT LOG INSERT
        INSERT INTO pipeline_audit_log (
            pipeline_name,
            pipeline_run_id,
            layer_name,
            source_table,
            watermark_start,
            watermark_end,
            rows_processed,
            status,
            data_path,
            run_end_time
        )
        VALUES (
            @pipeline_name,
            @pipeline_run_id,
            'bronze',
            @source_table,
            @old_watermark,
            @new_watermark_value,
            @rows_copied,
            'SUCCESS',
            @bronze_path,
            GETUTCDATE()
        );

    END TRY
    BEGIN CATCH

        INSERT INTO pipeline_audit_log (
            pipeline_name,
            pipeline_run_id,
            layer_name,
            source_table,
            status,
            error_message,
            run_end_time
        )
        VALUES (
            @pipeline_name,
            @pipeline_run_id,
            'bronze',
            @source_table,
            'FAILED',
            ERROR_MESSAGE(),
            GETUTCDATE()
        );

        THROW;
    END CATCH
END
GO







CREATE OR ALTER PROCEDURE usp_update_ingestion_watermark
    @source_system        VARCHAR(50),
    @source_table         VARCHAR(100),
    @new_watermark_value  DATETIME2,
    @pipeline_name        VARCHAR(100),
    @pipeline_run_id      VARCHAR(100),
    @rows_copied          INT,
    @bronze_path          VARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        DECLARE @old_watermark DATETIME2;

        -- Get previous watermark
        SELECT @old_watermark = last_watermark_value
        FROM ingestion_watermark
        WHERE source_system = @source_system
          AND source_table_name = @source_table;

        -- Update watermark
        UPDATE ingestion_watermark
        SET
            last_watermark_value = @new_watermark_value,
            last_run_status      = 'SUCCESS',
            last_run_rows_copied = @rows_copied,
            last_run_end_time    = GETUTCDATE(),
            pipeline_name        = @pipeline_name,
            pipeline_run_id      = @pipeline_run_id,
            bronze_path          = @bronze_path,
            updated_at           = GETUTCDATE()
        WHERE source_system = @source_system
          AND source_table_name = @source_table;

        -- Insert audit log
        INSERT INTO pipeline_audit_log (
            pipeline_name,
            pipeline_run_id,
            layer_name,
            source_table,
            watermark_start,
            watermark_end,
            rows_processed,
            status,
            data_path,
            run_end_time
        )
        VALUES (
            @pipeline_name,
            @pipeline_run_id,
            'bronze',
            @source_table,
            @old_watermark,
            @new_watermark_value,
            @rows_copied,
            'SUCCESS',
            @bronze_path,
            GETUTCDATE()
        );

    END TRY
    BEGIN CATCH

        INSERT INTO pipeline_audit_log (
            pipeline_name,
            pipeline_run_id,
            layer_name,
            source_table,
            status,
            error_message,
            run_end_time
        )
        VALUES (
            @pipeline_name,
            @pipeline_run_id,
            'bronze',
            @source_table,
            'FAILED',
            ERROR_MESSAGE(),
            GETUTCDATE()
        );

        THROW;

    END CATCH
END
GO

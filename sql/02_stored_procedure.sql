-- ============================================================
-- 02_stored_procedure.sql
-- sp_aggregate_team_metrics
-- Aggregates fact_daily_metrics into agg_team_daily for a
-- given execution date. Idempotent via MERGE — safe to re-run.
-- Called by ADF Execute Stored Procedure activity.
-- ============================================================

CREATE OR ALTER PROCEDURE sp_aggregate_team_metrics
    @ExecutionDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @date_id INT = CONVERT(INT, FORMAT(@ExecutionDate, 'yyyyMMdd'));
    DECLARE @rows_affected INT = 0;
    DECLARE @start_time DATETIME = GETDATE();

    BEGIN TRY

        MERGE agg_team_daily AS target
        USING (
            SELECT
                f.team_id,
                f.date_id,
                SUM(f.items_handled)                        AS total_items_handled,
                AVG(CAST(f.aht_seconds AS DECIMAL(10,2)))  AS avg_aht,
                AVG(f.csat_score)                           AS avg_csat,
                AVG(f.resolution_rate)                      AS avg_resolution_rate,
                AVG(f.first_call_resolution)                AS avg_fcr,
                AVG(f.concurrent_chats_avg)                 AS avg_concurrent_chats,
                AVG(f.avg_response_hours)                   AS avg_response_hours,
                AVG(f.sla_compliance)                       AS avg_sla_compliance,
                AVG(f.avg_days_to_close)                    AS avg_days_to_close,
                AVG(f.reopen_rate)                          AS avg_reopen_rate
            FROM fact_daily_metrics f
            WHERE f.date_id = @date_id
            GROUP BY f.team_id, f.date_id
        ) AS source
        ON target.team_id = source.team_id AND target.date_id = source.date_id

        WHEN MATCHED THEN UPDATE SET
            total_items_handled  = source.total_items_handled,
            avg_aht              = source.avg_aht,
            avg_csat             = source.avg_csat,
            avg_resolution_rate  = source.avg_resolution_rate,
            avg_fcr              = source.avg_fcr,
            avg_concurrent_chats = source.avg_concurrent_chats,
            avg_response_hours   = source.avg_response_hours,
            avg_sla_compliance   = source.avg_sla_compliance,
            avg_days_to_close    = source.avg_days_to_close,
            avg_reopen_rate      = source.avg_reopen_rate

        WHEN NOT MATCHED BY TARGET THEN INSERT (
            team_id, date_id, total_items_handled, avg_aht, avg_csat,
            avg_resolution_rate, avg_fcr, avg_concurrent_chats,
            avg_response_hours, avg_sla_compliance, avg_days_to_close, avg_reopen_rate
        ) VALUES (
            source.team_id, source.date_id, source.total_items_handled, source.avg_aht,
            source.avg_csat, source.avg_resolution_rate, source.avg_fcr,
            source.avg_concurrent_chats, source.avg_response_hours,
            source.avg_sla_compliance, source.avg_days_to_close, source.avg_reopen_rate
        );

        SET @rows_affected = @@ROWCOUNT;

        INSERT INTO pipeline_log (run_date, pipeline_name, status, rows_loaded, duration_secs)
        VALUES (
            @ExecutionDate,
            'sp_aggregate_team_metrics',
            'Success',
            @rows_affected,
            DATEDIFF(SECOND, @start_time, GETDATE())
        );

    END TRY
    BEGIN CATCH
        INSERT INTO pipeline_log (run_date, pipeline_name, status, rows_loaded, error_message, duration_secs)
        VALUES (
            @ExecutionDate,
            'sp_aggregate_team_metrics',
            'Failed',
            0,
            ERROR_MESSAGE(),
            DATEDIFF(SECOND, @start_time, GETDATE())
        );

        THROW;
    END CATCH
END;
GO

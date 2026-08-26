-- ============================================================
-- 03_pipeline_log.sql
-- Manual insert helper for ADF pipeline run logging.
-- The stored procedure handles its own logging via TRY/CATCH;
-- this file is for ad-hoc inserts and status queries.
-- ============================================================

-- View recent pipeline runs
SELECT TOP 20
    log_id,
    run_date,
    pipeline_name,
    status,
    rows_loaded,
    error_message,
    duration_secs,
    logged_at
FROM pipeline_log
ORDER BY logged_at DESC;

-- Count runs by status
SELECT
    status,
    COUNT(*)        AS run_count,
    AVG(duration_secs) AS avg_duration_secs
FROM pipeline_log
GROUP BY status;

-- Manual insert (use for testing or backfill)
-- INSERT INTO pipeline_log (run_date, pipeline_name, status, rows_loaded, duration_secs)
-- VALUES ('2025-01-01', 'pl_daily_team_aggregation', 'Success', 500, 12);

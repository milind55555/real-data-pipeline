-- PostgreSQL initialization for the IoT real-time pipeline.
-- This script runs automatically when the postgres container starts.
-- It creates the analytics table that Spark Structured Streaming writes
-- aggregated device metrics into.

CREATE TABLE IF NOT EXISTS device_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    device_id    TEXT NOT NULL,
    avg_voltage_v    DOUBLE PRECISION,
    avg_current_a    DOUBLE PRECISION,
    avg_power_w      DOUBLE PRECISION,
    avg_temp_c       DOUBLE PRECISION,
    avg_freq_hz      DOUBLE PRECISION,
    max_voltage_v    DOUBLE PRECISION,
    min_voltage_v    DOUBLE PRECISION,
    sample_count     BIGINT,
    processed_at     TIMESTAMPTZ DEFAULT now()
);

-- Index for fast time-range queries (Grafana dashboards)
CREATE INDEX IF NOT EXISTS idx_device_metrics_window
    ON device_metrics (window_start DESC);

-- Index for per-device lookups
CREATE INDEX IF NOT EXISTS idx_device_metrics_device
    ON device_metrics (device_id, window_start DESC);

-- Useful view: latest reading per device
CREATE OR REPLACE VIEW latest_device_metrics AS
SELECT DISTINCT ON (device_id)
    device_id, window_start, avg_voltage_v, avg_current_a,
    avg_power_w, avg_temp_c, avg_freq_hz, sample_count
FROM device_metrics
ORDER BY device_id, window_start DESC;

-- Grant read/write privileges to the pipeline user
GRANT SELECT, INSERT, UPDATE ON device_metrics TO pipeline;
GRANT SELECT, INSERT, UPDATE ON SEQUENCE device_metrics_id_seq TO pipeline;
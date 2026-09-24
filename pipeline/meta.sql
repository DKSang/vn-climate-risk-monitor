-- Control metadata for the start-time watermark pattern (docs/adr/0001).
CREATE SCHEMA IF NOT EXISTS meta;

-- One row per asset: the start time of its last successful job run.
-- Reprocess from time T with: UPDATE meta.watermarks SET watermark = T WHERE asset = '...';
CREATE TABLE IF NOT EXISTS meta.watermarks (
    asset      text PRIMARY KEY,
    watermark  timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One row per attempt, successful or not.
CREATE TABLE IF NOT EXISTS meta.job_runs (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    asset       text NOT NULL,
    status      text NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    watermark   timestamptz,
    rows_in     bigint,
    rows_out    bigint,
    error       text
);

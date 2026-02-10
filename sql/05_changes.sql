CREATE TABLE IF NOT EXISTS change_log (
    id SERIAL PRIMARY KEY,
    run_id INT NOT NULL REFERENCES ingestion_runs(id),
    market TEXT NOT NULL,
    ts_utc TIMESTAMPTZ NOT NULL,
    metric TEXT NOT NULL,
    prev_value NUMERIC,
    new_value NUMERIC,
    delta NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
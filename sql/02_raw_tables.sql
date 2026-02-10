CREATE TABLE IF NOT EXISTS raw_load (
    market TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    load_mw NUMERIC NOT NULL,
    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (market, ts, source_file)
);
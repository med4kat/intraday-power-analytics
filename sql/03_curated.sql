CREATE TABLE IF NOT EXISTS fact_intraday_state (
  market TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,

  load_mw NUMERIC,

  delta_prev_mw NUMERIC,
  delta_prev_pct NUMERIC,

  data_age_minutes NUMERIC,
  source_file TEXT,

  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (market, ts_utc)
);
CREATE OR REPLACE VIEW v_load_latest AS
SELECT DISTINCT ON (market)
  market,
  ts AS ts_utc,
  load_mw,
  forecast_load_mw,
  source_file,
  NOW() - ts AS age_interval
FROM raw_load
ORDER BY market, ts DESC;

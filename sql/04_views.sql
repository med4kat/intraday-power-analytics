CREATE OR REPLACE VIEW v_load_latest AS
SELECT DISTINCT ON (d.market_code)
  d.market_code AS market,
  r.ts AS ts_utc,
  r.load_mw,
  r.forecast_load_mw,
  r.source_file,
  NOW() - r.ts AS age_interval
FROM raw_load r
JOIN dim_market d
  ON r.market = d.raw_name
ORDER BY d.market_code, r.ts DESC;
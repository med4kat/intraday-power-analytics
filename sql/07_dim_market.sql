CREATE TABLE IF NOT EXISTS dim_market (
    raw_name TEXT PRIMARY KEY,
    market_code TEXT NOT NULL
);

INSERT INTO dim_market (raw_name, market_code) VALUES
('BZN|DE-LU', 'DE'),
('Germany (DE)', 'DE')
ON CONFLICT DO NOTHING;
# Intraday Power Analytics

A **desk-style intraday analytics tool** for European power markets.

## Business Problem

European power market data is published in multiple files, uses different time zones (CET/CEST), and is not immediately suitable for comparison across markets or reporting periods.

This project ingests and normalises ENTSO-E data into a consistent UTC-based model, allowing traders and analysts to quickly answer:

**Which markets changed, by how much, and where should I look first?**


## What it does
- Ingests ENTSO-E CSVs unchanged  
- Normalises **CET/CEST → UTC**  
- Stores full raw history  
- Reports **latest state, deltas, freshness, alerts**

## Run locally
**Python 3.11+, Docker**

```bash
docker compose up -d
python -m src.ingest.ingest_load_csv
python -m src.report.what_changed
```

To shut down the container and delete DBs:
```bash
docker compose down -v 
```

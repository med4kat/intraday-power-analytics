# Intraday Power Analytics

A **desk-style intraday analytics tool** for European power markets.

It ingests raw ENTSO-E data, converts timestamps to **UTC**, and shows **what changed**, **by how much**, and **whether it matters now**.

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
import csv
from pathlib import Path
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo

from src.db.connection import get_connection

INCOMING_DIR = Path("data/incoming")
ARCHIVE_DIR = Path("data/archive")

TZ_LOCAL = ZoneInfo("Europe/Berlin")
TZ_UTC = ZoneInfo("UTC")


def parse_mtu_to_utc(mtu_str: str) -> datetime:
    """
    Example:
    '05/02/2026 00:00 - 05/02/2026 00:15'
    ENTSO-E format: DD/MM/YYYY HH:MM
    We take the start of the interval.
    """
    start_str = mtu_str.split(" - ")[0].strip()
    local_dt = datetime.strptime(start_str,  "%d/%m/%Y %H:%M")
    local_dt = local_dt.replace(tzinfo=TZ_LOCAL)
    return local_dt.astimezone(TZ_UTC)


def ingest_file(file_path: Path) -> int:
    inserted = 0

    with file_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = {
            "MTU (CET/CEST)",
            "Area",
            "Actual Total Load (MW)",
            "Day-ahead Total Load Forecast (MW)"
        }

        if not required.issubset(reader.fieldnames or []):
            raise ValueError(
                f"{file_path.name} missing required columns: {required}"
            )

        with get_connection() as conn:
            with conn.cursor() as cur:
                for row in reader:
                    ts_utc = parse_mtu_to_utc(row["MTU (CET/CEST)"])
                    market = row["Area"].strip()
                    load_mw = row["Actual Total Load (MW)"].strip()
                    forecast = row["Day-ahead Total Load Forecast (MW)"].strip()


                    cur.execute(
                        """
                        INSERT INTO raw_load (market, ts, load_mw, forecast_load_mw, source_file)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            market,
                            ts_utc,
                            load_mw,
                            forecast,
                            file_path.name,
                        ),
                    )
                    inserted += cur.rowcount

            conn.commit()

    return inserted


def archive_file(file_path: Path) -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(file_path, ARCHIVE_DIR / file_path.name)


def main():
    csv_files = list(INCOMING_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found")
        return

    total = 0
    for file_path in csv_files:
        print(f"Ingesting {file_path.name}")
        rows = ingest_file(file_path)
        print(f"  rows inserted: {rows}")
        archive_file(file_path)
        total += rows

    print(f"Done. Total rows inserted: {total}")


if __name__ == "__main__":
    main()





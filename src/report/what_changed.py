from datetime import datetime, timezone, timedelta
from decimal import Decimal

from src.db.connection import get_connection


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1) create a new run record
            cur.execute(
                "INSERT INTO ingestion_runs (status, notes) VALUES (%s, %s) RETURNING id, run_ts;",
                ("ok", "what_changed report"),
            )
            run_id, run_ts = cur.fetchone()

            # 2) find previous run time (if any)
            cur.execute(
                """
                SELECT run_ts
                FROM ingestion_runs
                WHERE id <> %s
                ORDER BY run_ts DESC
                LIMIT 1;
                """,
                (run_id,),
            )
            prev = cur.fetchone()
            prev_run_ts = prev[0] if prev else (run_ts - timedelta(hours=24))

            # 3) for each market, compare latest raw point vs latest raw point before prev_run_ts
            cur.execute("SELECT DISTINCT market FROM raw_load ORDER BY market;")
            markets = [r[0] for r in cur.fetchall()]

            print("WHAT CHANGED SINCE LAST RUN")
            print(f"Run: {run_ts.isoformat()} | Previous: {prev_run_ts.isoformat()}")
            print("-" * 70)

            now = datetime.now(timezone.utc)

            for market in markets:
                # latest point
                cur.execute(
                    """
                    SELECT ts, load_mw, source_file
                    FROM raw_load
                    WHERE market = %s
                    ORDER BY ts DESC
                    LIMIT 1;
                    """,
                    (market,),
                )
                latest = cur.fetchone()
                if not latest:
                    continue
                latest_ts, latest_load, latest_file = latest
                age_min = (now - latest_ts).total_seconds() / 60.0

                # point before previous run
                cur.execute(
                    """
                    SELECT ts, load_mw
                    FROM raw_load
                    WHERE market = %s AND ts <= %s
                    ORDER BY ts DESC
                    LIMIT 1;
                    """,
                    (market, prev_run_ts),
                )
                before = cur.fetchone()
                before_load = before[1] if before else None

                delta = (latest_load - before_load) if before_load is not None else None
                delta_pct = (
                    (delta / before_load * Decimal("100"))
                    if (delta is not None and before_load)
                    else None
                )

                # print
                print(f"{market} | latest {latest_ts.isoformat()} | load={latest_load} MW | age={age_min:.1f} min")
                if before_load is None:
                    print("     no previous baseline (first run or no older data)")
                else:
                    print(f"     prev load={before_load} MW | Δ={delta:+.0f} MW ({delta_pct:+.2f}%)")

                # simple alerts
                if age_min > 30:
                    print("     ALERT: data stale (>30 min)")
                if delta is not None and abs(delta) >= Decimal("1500"):
                    print("     ALERT: large move (>=1500 MW)")

                # log to change_log (optional but nice)
                cur.execute(
                    """
                    INSERT INTO change_log (run_id, market, ts_utc, metric, prev_value, new_value, delta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (run_id, market, latest_ts, "load_mw", before_load, latest_load, delta),
                )

                print(f"     source: {latest_file}")
                print()

        conn.commit()


if __name__ == "__main__":
    main()
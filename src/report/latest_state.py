from datetime import datetime, timezone

from src.db.connection import get_connection

from decimal import Decimal


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Latest load per market
            cur.execute("""
                SELECT market, ts_utc, load_mw, forecast_load_mw, source_file
                FROM v_load_latest
                ORDER BY market;
            """)
            rows = cur.fetchall()

            if not rows:
                print("No data found in v_load_latest")
                return

            print("LATEST STATE (raw → desk view)")
            print("-" * 60)

            now = datetime.now(timezone.utc)

            for market, ts_utc, load_mw, forecast_load_mw, source_file in rows:
                age_min = (now - ts_utc).total_seconds() / 60.0

                # previous interval value for delta
                cur.execute("""
                    SELECT load_mw
                    FROM raw_load
                    WHERE market = %s AND ts < %s
                    ORDER BY ts DESC
                    LIMIT 1;
                """, (market, ts_utc))
                prev = cur.fetchone()
                prev_load = prev[0] if prev else None

                delta = (load_mw - prev_load) if (prev_load is not None and load_mw is not None) else None
                delta_pct = (delta / prev_load * Decimal("100")) if (delta is not None and prev_load) else None

                surprise_mw = None
                surprise_pct = None
                if forecast_load_mw is not None and forecast_load_mw != 0:
                    surprise_mw = load_mw - forecast_load_mw
                    surprise_pct = (surprise_mw / forecast_load_mw) * Decimal("100")

                print(f"{market} | {ts_utc.isoformat()} | load={load_mw} MW | age={age_min:.1f} min")
                if delta is not None:
                    print(f"     Δ vs prev: {delta:+.0f} MW ({delta_pct:+.2f}%)")
                
                if forecast_load_mw is not None and surprise_mw is not None:
                    print(f"     DA forecast: {forecast_load_mw} MW")
                    print(f"     Surprise: {surprise_mw:+.0f} MW ({surprise_pct:+.2f}%)")
                    
                print(f"     source: {source_file}")

                # upsert into curated table
                cur.execute("""
                    INSERT INTO fact_intraday_state (
                        market, ts_utc, load_mw,
                        delta_prev_mw, delta_prev_pct,
                        data_age_minutes, source_file
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (market, ts_utc)
                    DO UPDATE SET
                        load_mw = EXCLUDED.load_mw,
                        delta_prev_mw = EXCLUDED.delta_prev_mw,
                        delta_prev_pct = EXCLUDED.delta_prev_pct,
                        data_age_minutes = EXCLUDED.data_age_minutes,
                        source_file = EXCLUDED.source_file,
                        computed_at = NOW();
                """, (market, ts_utc, load_mw, delta, delta_pct, age_min, source_file))

        conn.commit()


if __name__ == "__main__":
    main()
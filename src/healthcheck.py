from db.connection import get_connection

def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_runs (status, notes)
                VALUES (%s, %s)
                """,
                ("ok", "healthcheck run")
            )
            conn.commit()

    print("Healthcheck OK")

if __name__ == "__main__":
    main()
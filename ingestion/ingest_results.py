import os
import db_utils
import time
from dotenv import load_dotenv


load_dotenv("docker/.env")

START_SEASON = int(os.getenv("START_SEASON", 1950))
END_SEASON = int(os.getenv("END_SEASON", 2025))


def fetch_results_for_season(season):
    return db_utils.fetch_paginated(
        endpoint=f"/{season}/results.json",
        data_path=["MRData", "RaceTable", "Races"]
    )


def ingest_season(cur, season):
    races = fetch_results_for_season(season)

    inserted = 0

    for race in races:
        round_ = race["round"]
        race_id = f"{season}_{round_}"

        for res in race["Results"]:
            cur.execute("""
                INSERT INTO results_raw
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (race_id, driver_id) DO NOTHING;
            """, (
                race_id,
                int(season),
                int(round_),
                res["Driver"]["driverId"],
                res["Constructor"]["constructorId"],
                int(res["position"]),
                float(res["points"]),
                res.get("status")
            ))

            inserted += 1

    return inserted


def create_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results_raw (
            race_id TEXT,
            season INT,
            round INT,
            driver_id TEXT,
            constructor_id TEXT,
            position INT,
            points FLOAT,
            status TEXT,
            PRIMARY KEY (race_id, driver_id)
        );
    """)


def main():
    conn = db_utils.connect_db()
    cur = conn.cursor()

    create_table(cur)

    total_inserted = 0

    for season in range(START_SEASON, END_SEASON + 1):
        print(f"Ingesting season {season}...")

        try:
            inserted = ingest_season(cur, season)
            conn.commit()
            total_inserted += inserted

            print(
                f"Season {season} done "
                f"({inserted} results, total {total_inserted})"
            )

            time.sleep(2)  # cooldown between seasons

        except Exception as e:
            print(f"Season {season} failed: {e}")
            print("Cooling down for 5 minutes and stopping.")
            time.sleep(300)
            break

    cur.close()
    conn.close()

    print(f"Finished. Total results processed: {total_inserted}")


if __name__ == "__main__":
    main()
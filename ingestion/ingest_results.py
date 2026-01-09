import os
import requests
import psycopg2
from dotenv import load_dotenv
import time

load_dotenv()

START_SEASON = int(os.getenv("START_SEASON", 1950))
END_SEASON = int(os.getenv("END_SEASON", 2025))


def fetch_results_for_season(season):
    """
    Fetch ALL race results for a given season using pagination.
    Ergast defaults to limit=30, so we must paginate to get full seasons.
    """
    base_url = f"https://api.jolpi.ca/ergast/f1/{season}/results.json"

    headers = {
        "User-Agent": "F1DataEngineering/1.0",
        "Accept": "application/json"
    }

    limit = 100
    offset = 0
    all_races = []

    while True:
        params = {
            "limit": limit,
            "offset": offset
        }

        r = requests.get(base_url, headers=headers, params=params)

        if r.status_code == 429:
            raise Exception(f"Rate limited on season {season}")

        if r.status_code != 200:
            raise Exception(
                f"API request failed for season {season} "
                f"with status {r.status_code}"
            )

        data = r.json()
        races = data["MRData"]["RaceTable"]["Races"]

        if not races:
            break

        all_races.extend(races)
        offset += limit

        time.sleep(0.5)  # pagination pause

    return all_races


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


def connect_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


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
    conn = connect_db()
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
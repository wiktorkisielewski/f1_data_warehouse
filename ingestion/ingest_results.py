import os
import requests
import psycopg2
from dotenv import load_dotenv
import time

load_dotenv()


def fetch_all_results():
    base_url = "https://api.jolpi.ca/ergast/f1/results.json"

    headers = {
        "User-Agent": "F1DataEngineering/1.0",
        "Accept": "application/json"
    }

    limit = 30
    offset = 0
    all_results = []

    max_retries = 5
    retry_count = 0
    sleep_time = 2  # seconds

    while True:
        params = {"limit": limit, "offset": offset}
        r = requests.get(base_url, params=params, headers=headers)

        if r.status_code == 429:
            retry_count += 1

            if retry_count > max_retries:
                raise Exception(
                    f"Rate limited after {max_retries} retries. Aborting ingestion."
                )

            print(
            f"Rate limited ({retry_count}/{max_retries}). "
            f"Sleeping for {sleep_time} seconds..."
            )
            
            time.sleep(sleep_time)
            #exponential backoff capped at 60 sec
            sleep_time = min(sleep_time * 2, 60) 
            continue

        if r.status_code != 200:
            raise Exception(
                f"API request failed with status {r.status_code}\n{r.text}"
            )

        data = r.json()
        races = data["MRData"]["RaceTable"]["Races"]

        if not races:
            break

        for race in races:
            season = race["season"]
            round_ = race["round"]
            race_id = f"{season}_{round_}"

            for res in race["Results"]:
                all_results.append({
                    "race_id": race_id,
                    "season": int(season),
                    "round": int(round_),
                    "driver_id": res["Driver"]["driverId"],
                    "constructor_id": res["Constructor"]["constructorId"],
                    "position": int(res["position"]),
                    "points": float(res["points"]),
                    "status": res.get("status")
                })
                
        print(
            f"Accepted page: offset={offset}, "
            f"sleep_time={sleep_time}s, "
            f"total_results={len(all_results)}"
        )
        offset += limit
        retry_count = 0
        time.sleep(1.0)

    return all_results


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


def insert_results(cur, results):
    for r in results:
        cur.execute("""
            INSERT INTO results_raw
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (race_id, driver_id) DO NOTHING;
        """, (
            r["race_id"],
            r["season"],
            r["round"],
            r["driver_id"],
            r["constructor_id"],
            r["position"],
            r["points"],
            r["status"]
        ))


def main():
    results = fetch_all_results()
    conn = connect_db()
    cur = conn.cursor()

    create_table(cur)
    insert_results(cur, results)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(results)} race results")


if __name__ == "__main__":
    main()
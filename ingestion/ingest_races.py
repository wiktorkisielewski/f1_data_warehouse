import os
import requests
import psycopg2
import db_utils
from dotenv import load_dotenv

load_dotenv("docker/.env")


def fetch_all_races():
    base_url = "https://api.jolpi.ca/ergast/f1/races.json"

    headers = {
        "User-Agent": "F1DataEngineering/1.0",
        "Accept": "application/json"
    }

    limit = 100
    offset = 0
    all_races = []

    while True:
        params = {"limit": limit, "offset": offset}
        r = requests.get(base_url, params=params, headers=headers)

        if r.status_code != 200:
            raise Exception(
                f"API request failed with status {r.status_code}\n{r.text}"
            )

        data = r.json()
        races = data["MRData"]["RaceTable"]["Races"]

        if not races:
            break

        all_races.extend(races)
        offset += limit

    return all_races


def create_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS races_raw (
            race_id TEXT PRIMARY KEY,
            season INT,
            round INT,
            race_name TEXT,
            race_date DATE,
            race_time TIME,
            circuit_id TEXT,
            circuit_name TEXT,
            locality TEXT,
            country TEXT
        );
    """)


def insert_races(cur, races):
    for r in races:
        cur.execute("""
            INSERT INTO races_raw
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (race_id) DO NOTHING;
        """, (
            f"{r['season']}_{r['round']}",   # synthetic race_id
            int(r["season"]),
            int(r["round"]),
            r.get("raceName"),
            r.get("date"),
            r.get("time"),
            r["Circuit"]["circuitId"],
            r["Circuit"]["circuitName"],
            r["Circuit"]["Location"].get("locality"),
            r["Circuit"]["Location"].get("country")
        ))


def main():
    races = fetch_all_races()
    conn = db_utils.connect_db()
    cur = conn.cursor()

    create_table(cur)
    insert_races(cur, races)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(races)} races")


if __name__ == "__main__":
    main()
import os
import requests
import psycopg2
import db_utils
from dotenv import load_dotenv

load_dotenv("docker/.env")

def fetch_all_drivers():
    base_url = "https://api.jolpi.ca/ergast/f1/drivers.json"

    headers = {
        "User-Agent": "F1DataEngineering/1.0",
        "Accept": "application/json"
    }

    limit = 100
    offset = 0
    drivers = []

    while True:
        r = requests.get(
            base_url,
            headers=headers,
            params={"limit": limit, "offset": offset}
        )

        if r.status_code != 200:
            raise Exception(
                f"API request failed ({r.status_code}): {r.text}"
            )

        data = r.json()
        batch = data["MRData"]["DriverTable"]["Drivers"]

        if not batch:
            break

        drivers.extend(batch)
        offset += limit
        time.sleep(0.2)

    return drivers


def create_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS drivers_raw (
            driver_id TEXT PRIMARY KEY,
            code TEXT,
            given_name TEXT,
            family_name TEXT,
            nationality TEXT,
            dob DATE
        );
    """)

def insert_drivers(cur, drivers):
    for d in drivers:
        cur.execute("""
            INSERT INTO drivers_raw
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (driver_id) DO NOTHING;
        """, (
            d["driverId"],
            d.get("code"),
            d.get("givenName"),
            d.get("familyName"),
            d.get("nationality"),
            d.get("dateOfBirth")
        ))

def main():
    drivers = fetch_all_drivers()
    conn = db_utils.connect_db()
    cur = conn.cursor()

    create_table(cur)
    insert_drivers(cur, drivers)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(drivers)} drivers")

if __name__ == "__main__":
    main()
import os
import requests
import psycopg2
import db_utils
import time
from dotenv import load_dotenv

load_dotenv("docker/.env")

def fetch_all_drivers():
    return db_utils.fetch_paginated(
        endpoint="/drivers.json",
        data_path=["MRData", "DriverTable", "Drivers"]
    )


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
import requests
import psycopg2


def fetch_drivers():
    URL = "https://api.jolpi.ca/ergast/f1/drivers.json?limit=100"

    headers = {
        "User-Agent": "F1DataEngineering/1.0",
        "Accept": "application/json"
    }

    r = requests.get(URL, headers=headers)

    if r.status_code != 200:
        raise Exception(
            f"API request failed with status {r.status_code}\nResponse: {r.text}"
        )

    data = r.json()
    return data["MRData"]["DriverTable"]["Drivers"]

def connect_db():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="f1_raw",
        user="f1_user",
        password="f1_password"
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
    drivers = fetch_drivers()
    conn = connect_db()
    cur = conn.cursor()

    create_table(cur)
    insert_drivers(cur, drivers)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(drivers)} drivers")

if __name__ == "__main__":
    main()
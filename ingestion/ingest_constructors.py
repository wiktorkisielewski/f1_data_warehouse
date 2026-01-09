import os
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def fetch_all_constructors():
    base_url = "https://api.jolpi.ca/ergast/f1/constructors.json"

    headers = {
        "User-Agent": "F1DataEngineering/1.0",
        "Accept": "application/json"
    }

    limit = 100
    offset = 0
    all_constructors = []

    while True:
        params = {"limit": limit, "offset": offset}
        r = requests.get(base_url, params=params, headers=headers)

        if r.status_code != 200:
            raise Exception(
                f"API request failed with status {r.status_code}\n{r.text}"
            )

        data = r.json()
        constructors = data["MRData"]["ConstructorTable"]["Constructors"]

        if not constructors:
            break

        all_constructors.extend(constructors)
        offset += limit

    return all_constructors


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
        CREATE TABLE IF NOT EXISTS constructors_raw (
            constructor_id TEXT PRIMARY KEY,
            name TEXT,
            nationality TEXT,
            url TEXT
        );
    """)


def insert_constructors(cur, constructors):
    for c in constructors:
        cur.execute("""
            INSERT INTO constructors_raw
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (constructor_id) DO NOTHING;
        """, (
            c["constructorId"],
            c.get("name"),
            c.get("nationality"),
            c.get("url")
        ))


def main():
    constructors = fetch_all_constructors()
    conn = connect_db()
    cur = conn.cursor()

    create_table(cur)
    insert_constructors(cur, constructors)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(constructors)} constructors")


if __name__ == "__main__":
    main()
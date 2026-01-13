import db_utils

def fetch_all_races():
    return db_utils.fetch_paginated(
        endpoint="/races.json",
        data_path=["MRData","RaceTable","Races"]
    )


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
import db_utils

def fetch_all_constructors():
    return db_utils.fetch_paginated(
        endpoint="/constructors.json",
        data_path=["MRData","ConstructorTable","Constructors"]
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
    conn = db_utils.connect_db()
    cur = conn.cursor()

    create_table(cur)
    insert_constructors(cur, constructors)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(constructors)} constructors")


if __name__ == "__main__":
    main()
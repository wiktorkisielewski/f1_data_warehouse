import db_utils

logger = db_utils.setup_logger("ingest_constructors")


def fetch_all_constructors():
    logger.info("Fetching constructors from API")
    constructors = db_utils.fetch_paginated(
        endpoint="/constructors.json",
        data_path=["MRData", "ConstructorTable", "Constructors"]
    )
    logger.info(f"Fetched {len(constructors)} constructors")
    return constructors


def create_table(cur):
    logger.info("Ensuring constructors_raw table exists")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS constructors_raw (
            constructor_id TEXT PRIMARY KEY,
            name TEXT,
            nationality TEXT,
            url TEXT
        );
    """)


def insert_constructors(cur, constructors):
    logger.info("Inserting constructors into database")
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
    logger.info("Constructor insert completed")


def main():
    logger.info("Starting constructors ingestion")

    conn = db_utils.connect_db()
    cur = conn.cursor()

    try:
        constructors = fetch_all_constructors()
        create_table(cur)
        insert_constructors(cur, constructors)
        conn.commit()

        logger.info(f"Loaded {len(constructors)} constructors successfully")

    except Exception as e:
        conn.rollback()
        logger.exception("Fatal error during constructors ingestion")
        raise

    finally:
        cur.close()
        conn.close()
        logger.info("Constructors ingestion finished")


if __name__ == "__main__":
    main()
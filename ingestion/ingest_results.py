import os
import time
from ingestion import db_utils
from dotenv import load_dotenv

load_dotenv("docker/.env")

logger = db_utils.setup_logger("ingest_results")

START_SEASON = int(os.getenv("START_SEASON", 1950))
END_SEASON = int(os.getenv("END_SEASON", 2025))

MAX_RETRIES_PER_SEASON = 3


def fetch_results_for_season(season):
    logger.debug(f"Fetching results for season {season}")
    return db_utils.fetch_paginated(
        endpoint=f"/{season}/results.json",
        data_path=["MRData", "RaceTable", "Races"]
    )


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


def create_table(cur):
    logger.debug("Ensuring results_raw table exists")
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
    logger.info("Starting historical results ingestion")

    conn = db_utils.connect_db()
    cur = conn.cursor()

    try:
        create_table(cur)

        total_inserted = 0

        for season in range(START_SEASON, END_SEASON + 1):
            retries = 0

            while True:
                logger.info(
                    f"Ingesting season {season} "
                    f"(attempt {retries + 1}/{MAX_RETRIES_PER_SEASON})"
                )

                try:
                    inserted = ingest_season(cur, season)
                    conn.commit()
                    total_inserted += inserted

                    logger.info(
                        f"Season {season} completed "
                        f"({inserted} rows, total {total_inserted})"
                    )

                    time.sleep(2)  # gentle pacing
                    break  # ✅ move to next season

                except db_utils.RateLimitExceeded as e:
                    retries += 1
                    logger.warning(str(e))

                    if retries >= MAX_RETRIES_PER_SEASON:
                        logger.error(
                            f"Season {season} exceeded max retries "
                            f"({MAX_RETRIES_PER_SEASON}). Aborting ingestion."
                        )
                        return

                    logger.warning(
                        "API rate limit reached. "
                        f"Cooling down for 5 minutes before retrying season {season}"
                    )
                    time.sleep(300)
                    logger.info(f"Retrying season {season} after cooldown")

                except Exception:
                    logger.exception(
                        f"Fatal error while ingesting season {season}"
                    )
                    return

        logger.info(
            f"Ingestion finished successfully. "
            f"Total results processed: {total_inserted}"
        )

    finally:
        cur.close()
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()
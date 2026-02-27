import os
import time
from ingestion.db import connect_db
from ingestion.api import fetch_paginated, RateLimitExceeded
from ingestion.logger import setup_logger
from dotenv import load_dotenv
from ingestion.progress_display import render_progress, render_cooldown, reset_progress_display

load_dotenv("docker/.env")

logger = setup_logger("ingest_results")

START_SEASON = int(os.getenv("START_SEASON", 1950))
END_SEASON = int(os.getenv("END_SEASON", 2025))

MAX_RETRIES_PER_SEASON = 3


def fetch_results_for_season(season):
    logger.debug(f"Fetching results for season {season}")
    return fetch_paginated(
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

    conn = connect_db()
    cur = conn.cursor()

    try:
        create_table(cur)

        total_inserted = 0

        # Prepare season list for progress tracking
        seasons = list(range(START_SEASON, END_SEASON + 1))
        total_seasons = len(seasons)

        for idx, season in enumerate(seasons, start=1):
            retries = 0

            while True:
                try:
                    inserted = ingest_season(cur, season)
                    conn.commit()

                    total_inserted += inserted

                    # Render live progress
                    render_progress(idx, total_seasons, total_inserted)

                    time.sleep(1)  # smooth pacing
                    break

                except RateLimitExceeded as e:
                    retries += 1
                    logger.warning(str(e))

                    if retries >= MAX_RETRIES_PER_SEASON:
                        logger.error(
                            f"Season {season} exceeded max retries "
                            f"({MAX_RETRIES_PER_SEASON}). Aborting ingestion."
                        )
                        return

                    # Render live cooldown timer
                    render_cooldown(300)
                    reset_progress_display()

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
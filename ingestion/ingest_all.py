from pathlib import Path
import logging

from ingestion.wait_for_db import main as wait_for_db
from ingestion.ingest_drivers import main as ingest_drivers
from ingestion.ingest_constructors import main as ingest_constructors
from ingestion.ingest_races import main as ingest_races
from ingestion.ingest_results import main as ingest_results

LOG = logging.getLogger("ingest_all")

def main():
    LOG.info("Starting full ingestion pipeline")

    steps = [
        ("wait_for_db", wait_for_db),
        ("drivers", ingest_drivers),
        ("constructors", ingest_constructors),
        ("races", ingest_races),
        ("results", ingest_results),
    ]

    for name, step in steps:
        LOG.info(f"Running step: {name}")
        step()
        LOG.info(f"Finished step: {name}")

    # SIGNAL COMPLETION FOR DBT
    Path("/tmp/INGESTION_DONE").touch()
    LOG.info("Ingestion completed successfully. Marker file created.")

if __name__ == "__main__":
    main()
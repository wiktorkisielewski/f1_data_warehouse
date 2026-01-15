from ingestion.wait_for_db import wait_for_db
from ingestion.ingest_drivers import main as ingest_drivers
from ingestion.ingest_constructors import main as ingest_constructors
from ingestion.ingest_races import main as ingest_races
from ingestion.ingest_results import main as ingest_results

def main():
    for step in [
        wait_for_db,
        ingest_drivers,
        ingest_constructors,
        ingest_races,
        ingest_results,
    ]:
        step()

if __name__ == "__main__":
    main()
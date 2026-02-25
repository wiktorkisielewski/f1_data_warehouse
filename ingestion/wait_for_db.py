import time
import psycopg2
import os
import logging

LOG = logging.getLogger("wait_for_db")

def main():
    LOG.info("Waiting for Postgres...")

    while True:
        try:
            psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                dbname=os.getenv("POSTGRES_DB"),
            )
            LOG.info("Postgres is ready!")
            break
        except psycopg2.OperationalError:
            LOG.info("Postgres not ready yet, retrying...")
            time.sleep(2)
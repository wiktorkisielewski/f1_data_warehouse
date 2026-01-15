import time
import psycopg2
import os

def wait_for_db():
    while True:
        try:
            psycopg2.connect(
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                database=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
            ).close()
            print("Postgres is ready!")
            break
        except Exception:
            print("Waiting for Postgres...")
            time.sleep(2)
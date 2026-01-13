import os
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# -------------------------
# Database
# -------------------------


def connect_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "f1_db"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
    )

# -------------------------
# API Config
# -------------------------


F1_API_BASE_URL = os.getenv(
    "F1_API_BASE_URL",
    "https://api.jolpi.ca/ergast/f1"
)

HEADERS = {
    "User-Agent": "F1DataEngineering/1.0",
    "Accept": "application/json"
}

# Rate limits
MAX_REQUESTS_PER_SECOND = 4
MIN_REQUEST_INTERVAL = 1 / MAX_REQUESTS_PER_SECOND # 0.25s
HARD_RATE_LIMIT_SLEEP = 60 # seconds (cooldown on 429)

def fetch_paginated(endpoint, data_path):
    """
    Generic paginated fetcher for Ergast-style APIs.

    :param endpoint: API endpoint (e.g. "/drivers.json")
    :param data_path: list of keys to reach the records
                      (e.g. ["MRData", "DriverTable", "Drivers"])
    """
    limit = 100
    offset = 0
    results = []

    last_request_time = 0

    while True:
        elapsed = time.time() - last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)

        url = f"{F1_API_BASE_URL}{endpoint}"

        response = requests.get(
            url,
            headers=HEADERS,
            params={"limit": limit, "offset": offset}
        )

        last_request_time = time.time()

        if response.status_code == 429:
            print("⚠️ Rate limited. Cooling down...")
            time.sleep(HARD_RATE_LIMIT_SLEEP)
            continue

        response.raise_for_status()

        data = response.json()

        batch = data
        for key in data_path:
            batch = batch[key]

        if not batch:
            break

        results.extend(batch)
        offset += limit

    return results
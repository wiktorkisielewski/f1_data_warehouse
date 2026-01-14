import os
import logging
import time
import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# -------------------------
# Logging
# -------------------------

LOG_DIR = os.getenv("LOG_DIR", "ingestion/logs")
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger  # prevent duplicate handlers

    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(LOG_DIR, f"{name}_{timestamp}.log")

    formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

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

class RateLimitExceeded(Exception):
    """Raised when API rate limit is hit and ingestion should pause."""


def fetch_paginated(endpoint, data_path):
    """
    Generic paginated fetcher for Ergast-style APIs.

    :param endpoint: API endpoint (e.g. "/drivers.json")
    :param data_path: list of keys to reach the records
    """
    limit = 100
    offset = 0
    results = []

    last_request_time = 0

    while True:
        # Enforce rate limit
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

        # 🚦 Rate limit handling
        if response.status_code == 429:
            raise RateLimitExceeded(
                "API rate limit exceeded (HTTP 429). Cooldown required."
            )

        # 🛑 Transient server errors (retryable)
        if response.status_code in {500, 502, 503, 504}:
            raise RateLimitExceeded(
                f"Server error {response.status_code}. Temporary API failure."
            )

        # ❌ Non-retryable errors
        response.raise_for_status()

        # ✅ Success path
        data = response.json()

        batch = data
        for key in data_path:
            batch = batch[key]

        if not batch:
            break

        results.extend(batch)
        offset += limit

    return results
# 🏎️ Formula 1 Data Warehouse

An end-to-end data engineering project that ingests, models, and transforms historical Formula 1 data into an analytics-ready warehouse.

The project demonstrates modern data engineering and analytics engineering best practices, including robust ingestion, layered transformations, data quality testing, and star-schema modeling.

---

## Overview
This warehouse enables analysis of Formula 1 history such as:
- Driver career performance
- Constructor dominance by season
- Race and calendar analytics

The pipeline is designed to be **idempotent, resumable, and analytics-ready**, closely mirroring real-world production patterns.

---

## Tech Stack
- **Python** – API ingestion & orchestration
- **PostgreSQL** – relational warehouse (Dockerized)
- **Docker & Docker Compose** – local infrastructure
- **dbt** – transformations, testing, and modeling
- **SQL** – analytics & data modeling
- **Public Ergast API** (via `api.jolpi.ca` mirror)

---

## Architecture

### Ingestion
- Python ingestion scripts pull data from the Ergast F1 API
- Explicit handling of:
  - API pagination
  - Rate limits
  - Seasonal batch ingestion
- Pipelines are **idempotent** and safe to re-run

### Storage
- PostgreSQL running locally in Docker
- Separate raw and analytics layers

### Transformation
- dbt used to implement a layered transformation approach:
  - **Raw → Staging → Marts**
- Data quality enforced via dbt tests
- Star schema modeled for analytics consumption

---

## Data Model

### Raw Tables
- `drivers_raw`
- `constructors_raw`
- `races_raw`
- `results_raw`

### Staging Models (dbt)
- `stg_drivers`
- `stg_constructors`
- `stg_races`
- `stg_results`

### Analytics Marts
**Dimensions**
- `dim_drivers`
- `dim_constructors`
- `dim_races`

**Fact**
- `fact_results`  
  *(one row per driver per race)*

---

## Data Quality & Testing
- dbt tests implemented for:
  - `not_null`
  - `unique`
  - `relationships` (foreign key integrity)
- Ensures referential integrity between facts and dimensions

---

## Warehouse Schema

![Warehouse Schema](docs/lineage.png)

---

## Example Analytics
Example SQL analytics queries are available in `f1_dbt/analyses/`, including:
- Top drivers by career points
- Constructor dominance by season
- Driver win counts
- Race distribution by country

---

## Prerequisites

Before running the project locally, ensure you have the following installed:

- **Git**
- **Docker & Docker Compose**
  - https://docs.docker.com/get-docker/
- **Python 3.13+**
  - Recommended to use `pyenv` or system Python
- **pip**
- **dbt-postgres**
  ```bash
  pip install dbt-postgres
  ```
---
## How to Run Locally

### 1️⃣ Local Environment Setup

  Clone the repository

  ```bash
  git clone https://github.com/SebastianSwiczerewski/f1_data_warehouse.git
  cd f1_data_warehouse/
  ```

  Create a local .env file from the example provided and populate the values

  ```bash
  cp .env.example .env
  ```
  
  Install Python dependencies

  ```bash
  pip install -r requirements.txt
  ```


### 2️⃣ Start PostgreSQL with Docker
  The project includes a preconfigured docker-compose.yml. No additional Docker setup is required 

  ```bash
  cd docker/
  docker compose up -d
  ```

  Verify the contaner is running

  ```bash
  docker ps 
  ```

### 3️⃣ Run ingestion script for raw tables to populate Postgres

  This ingests historical Formula 1 data into PostgreSQL
  Execution time for ingest_results.py depends on API rate limits and includes automatic cooldowns and retries
  Due to API rate limits the ingestion process may take up to 30min

  ```bash
  cd .. # f1_data_warehouse/ 
  python -m ingestion.ingest_all # to run all ~27min, 28134 rows
  ```

  Optionally to run separate ingestion files

  ```bash
  python ingestion/ingest_drivers.py # ~ 6sec, 874 rows
  python ingestion/ingest_constructors.py # ~ 2sec, 214 rows
  python ingestion/ingest_races.py # ~ 9sec, 1173 rows
  python ingestion/ingest_results.py # ~ 25min, 25873 rows
  ```

### 4️⃣ Setup dbt connection

  ```bash
  cd f1_dbt
  dbt init
  ```

  Use the following values when prompted

  database: postgres or choose number 1 \
  host:  same as in .env \
  port: same as in .env \
  user: same as in .env \
  password: same as in .env \
  dbname: same as in .env \
  schema: public \
  threads: 4 

  Test out the connection

  ```bash
  dbt debug
  ```

### 5️⃣ Run dbt models & tests

  Run a single model:

  ```bash
  dbt run --select stg_drivers
  ```

  Run all staging models:

  ```bash
  dbt run --select staging
  ```

  Run all dimension models:
  
  ```bash
  dbt run --select marts
  ```

### 6️⃣ Run dbt tests

  ```bash
  dbt test
  ```

### 7️⃣ Analytics & Validation

  ```bash
  cd docker/
  docker exec -it f1_postgres psql -U f1_user -d f1_raw
  ```

  Useful commands

  ```bash
  \d                  # List of relations
  \dt                 # List tables
  \dv                 # List views
  \d table_name       # Describe table
  \q                  # Exit
  ```

  For example analytics queries using fact and dimension models check ./f1_dbt/analyses/f1_analytics.sql


### 8️⃣ Tear Down Local Environment

  Stop Docker services (keep data)

  ```bash
  docker compose down
  ```

  ⚠️ WARNING: This deletes data

  ```bash
  docker compose down -v
  ```
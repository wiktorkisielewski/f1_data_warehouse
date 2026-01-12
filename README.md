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

### 1️⃣ Clone the repository

  ```bash
  git clone https://github.com/SebastianSwiczerewski/f1_data_warehouse.git
  cd f1_data_warehouse/
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

  Stop services (keep data)

  ```bash
  docker compose down
  ```

  ⚠️ WARNING: This deletes data

  ```bash
  docker compose down -v
  ```

### 3️⃣ Run ingestion scripts for raw tables to populate Postgres

  ```bash
  cd f1_data_warehouse/
  python ingestion/ingest_drivers.py
  python ingestion/ingest_constructors.py
  python ingestion/ingest_races.py
  python ingestion/ingest_results.py
  ```

  This ingests historical Formula 1 data into PostgreSQL

### 4️⃣ Setup dbt connection

  ```bash
  cd f1_dbt
  dbt debug
  ```

  Use the following values when prompted

  database: postgres \
  host: localhost \
  port: 5433 \
  user: f1_user \
  password: f1_password \
  dbname: f1_raw \
  schema: public \
  threads: 4 

### 5️⃣ Run dbt models & tests

  Run a single model:

  ```bash
  dbt run --select stg_drivers
  ```

  Run all staging models:

  ```bash
  dbt run --select staging
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

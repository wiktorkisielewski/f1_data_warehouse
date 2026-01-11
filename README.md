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

## Example Analytics
- Top drivers by total career points
- Constructor dominance by season
- Wins per driver
- Historical race calendar analysis

(Example queries included in `analyses/`.)

---

## How to Run Locally

```bash
# Start Postgres
docker compose up -d

# Run dbt models & tests
dbt run
dbt test
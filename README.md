# F1 Data Warehouse 🏎️

End-to-end data engineering project that ingests, models, and transforms Formula 1 data into an analytics-ready warehouse.

## Tech Stack
- Python (data ingestion)
- PostgreSQL (Dockerized)
- dbt (transformations & modeling)
- Public Ergast API (via api.jolpi.ca)

## Architecture
- Raw ingestion layer built in Python
- Relational storage in PostgreSQL
- Incremental, idempotent batch ingestion
- Transformation layer implemented with dbt

## Ingestion Strategy
- Drivers, constructors, and races ingested as dimension tables
- Race results ingested in **seasonal batches** to respect public API rate limits
- API pagination and throttling handled explicitly
- Pipeline is **resumable** and safe to re-run

## Data Model (High Level)
- `drivers_raw`
- `constructors_raw`
- `races_raw`
- `results_raw` (fact table)

## Future Work
- dbt staging models
- Analytics marts (driver performance, constructor dominance)
- BI dashboards
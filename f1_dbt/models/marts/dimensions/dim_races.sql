select
    race_id,
    season,
    race_round,
    race_name,
    race_date,
    race_time,
    circuit_id,
    circuit_name,
    locality,
    country
from {{ ref('stg_races') }}
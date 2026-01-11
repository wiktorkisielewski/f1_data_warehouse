select
    r.race_id,
    r.season,
    r.race_round,
    r.driver_id,
    r.constructor_id,
    r.finish_position,
    r.points,
    r.status
from {{ ref('stg_results') }} as r
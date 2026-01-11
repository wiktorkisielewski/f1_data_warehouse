select
    driver_id,
    first_name,
    last_name,
    nationality,
    date_of_birth
from {{ ref('stg_drivers') }}
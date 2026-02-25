WITH base AS (

    SELECT *
    FROM {{ ref('fact_results') }}

),

drivers AS (

    SELECT
        driver_id,
        first_name,
        last_name
    FROM {{ ref('dim_drivers') }}

),

constructors AS (

    SELECT
        constructor_id,
        constructor_name
    FROM {{ ref('dim_constructors') }}

)

SELECT
    b.race_id,
    b.season,
    b.race_round,

    b.driver_id,
    d.first_name || ' ' || d.last_name AS driver_name,

    b.constructor_id,
    c.constructor_name,

    b.finish_position,
    b.points,
    b.status

FROM base b
LEFT JOIN drivers d
    ON b.driver_id = d.driver_id
LEFT JOIN constructors c
    ON b.constructor_id = c.constructor_id
WITH base AS (

    SELECT
        season,
        decade,
        driver_id,
        constructor_id,
        total_driver_titles,
        total_constructor_titles
    FROM {{ ref('fact_championships') }}

)

SELECT
    b.season,
    b.decade,
    b.driver_id,
    b.constructor_id,
    d.first_name || ' ' || d.last_name AS driver_name,
    c.constructor_name,
    b.total_driver_titles,
    b.total_constructor_titles

FROM base b

LEFT JOIN {{ ref('dim_drivers') }} d
    ON b.driver_id = d.driver_id

LEFT JOIN {{ ref('dim_constructors') }} c
    ON b.constructor_id = c.constructor_id

ORDER BY b.season
WITH titles AS (

    SELECT
        driver_id,
        COUNT(*) AS total_driver_titles
    FROM {{ ref('fact_championships') }}
    GROUP BY driver_id

)

SELECT *
FROM titles
ORDER BY total_driver_titles DESC
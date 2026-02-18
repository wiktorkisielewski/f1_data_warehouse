WITH driver_titles AS (
    SELECT
        driver_id,
        COUNT(*) AS total_driver_titles
    FROM {{ ref('fact_championships') }}
    GROUP BY driver_id
),

driver_ranked AS (
    SELECT
        driver_id,
        total_driver_titles,
        DENSE_RANK() OVER (
            ORDER BY total_driver_titles DESC
        ) AS title_rank
    FROM driver_titles
)

SELECT
    fc.season,
    fc.driver_id,
    dr.total_driver_titles,
    dr.title_rank
FROM {{ ref('fact_championships') }} fc
JOIN driver_ranked dr
    ON fc.driver_id = dr.driver_id
ORDER BY dr.title_rank, fc.season
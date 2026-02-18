WITH base AS (
    SELECT
        driver_id,
        season,
        FLOOR(season / 10) * 10 AS decade
    FROM {{ ref('fact_championships') }}
),

titles_by_decade AS (
    SELECT
        driver_id,
        decade,
        COUNT(*) AS titles_in_decade
    FROM base
    GROUP BY driver_id, decade
),

total_titles AS (
    SELECT
        driver_id,
        COUNT(*) AS total_titles
    FROM {{ ref('fact_championships') }}
    GROUP BY driver_id
)

SELECT
    tbd.driver_id,
    tbd.decade,
    tbd.titles_in_decade,
    tt.total_titles
FROM titles_by_decade tbd
JOIN total_titles tt
    ON tbd.driver_id = tt.driver_id
ORDER BY tt.total_titles DESC, tbd.decade
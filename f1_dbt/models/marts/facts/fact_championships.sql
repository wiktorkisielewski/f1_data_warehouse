WITH driver_points AS (
    SELECT
        season,
        driver_id,
        SUM(points) AS total_points
    FROM {{ ref('fact_results') }}
    GROUP BY season, driver_id
),

driver_ranked AS (
    SELECT
        season,
        driver_id,
        total_points,
        ROW_NUMBER() OVER (
            PARTITION BY season
            ORDER BY total_points DESC
        ) AS rnk
    FROM driver_points
),

constructor_points AS (
    SELECT
        season,
        constructor_id,
        SUM(points) AS total_points
    FROM {{ ref('fact_results') }}
    GROUP BY season, constructor_id
),

constructor_ranked AS (
    SELECT
        season,
        constructor_id,
        total_points,
        ROW_NUMBER() OVER (
            PARTITION BY season
            ORDER BY total_points DESC
        ) AS rnk
    FROM constructor_points
),

champions AS (
    SELECT
        d.season,
        (d.season / 10) * 10 AS decade, 
        d.driver_id,
        c.constructor_id
    FROM driver_ranked d
    JOIN constructor_ranked c
        ON d.season = c.season
    WHERE d.rnk = 1
      AND c.rnk = 1
)

SELECT
    season,
    decade,
    driver_id,
    constructor_id,

    COUNT(*) OVER (PARTITION BY driver_id)
        AS total_driver_titles,

    COUNT(*) OVER (PARTITION BY constructor_id)
        AS total_constructor_titles

FROM champions
ORDER BY season
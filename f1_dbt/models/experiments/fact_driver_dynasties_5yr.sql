WITH champs AS (

    SELECT
        driver_id,
        season
    FROM {{ ref('fact_championships') }}

),

lagged AS (

    SELECT
        driver_id,
        season,
        LAG(season) OVER (
            PARTITION BY driver_id
            ORDER BY season
        ) AS prev_season
    FROM champs

),

streak_flags AS (

    SELECT *,
        CASE 
            WHEN season = prev_season + 1 THEN 0
            ELSE 1
        END AS new_streak
    FROM lagged

),

streak_groups AS (

    SELECT *,
        SUM(new_streak) OVER (
            PARTITION BY driver_id
            ORDER BY season
        ) AS streak_group
    FROM streak_flags

),

streak_lengths AS (

    SELECT
        driver_id,
        streak_group,
        MIN(season) AS start_season,
        MAX(season) AS end_season,
        COUNT(*) AS streak_length
    FROM streak_groups
    GROUP BY driver_id, streak_group
    HAVING COUNT(*) >= 2
)

SELECT
    driver_id,
    start_season,
    end_season,
    streak_length,
    (start_season / 5) * 5 AS season_5yr_bucket
FROM streak_lengths
ORDER BY start_season
WITH championships AS (

    SELECT
        season,
        driver_id
    FROM {{ ref('fact_championships') }}

),

ordered AS (

    SELECT
        driver_id,
        season,
        season - ROW_NUMBER() OVER (
            PARTITION BY driver_id
            ORDER BY season
        ) AS streak_group
    FROM championships

),

streaks AS (

    SELECT
        driver_id,
        MIN(season) AS start_season,
        MAX(season) AS end_season,
        COUNT(*) AS streak_length
    FROM ordered
    GROUP BY driver_id, streak_group

),

filtered AS (

    -- Only keep real dynasties (2+ consecutive titles)
    SELECT *
    FROM streaks
    WHERE streak_length >= 2

),

aggregated AS (

    SELECT
        driver_id,

        COUNT(*) AS number_of_dynasties,

        SUM(streak_length) AS total_dynasty_years,

        MAX(streak_length) AS longest_streak,

        MIN(start_season) AS first_dynasty_start

    FROM filtered
    GROUP BY driver_id

)

SELECT
    driver_id,
    number_of_dynasties,
    total_dynasty_years,
    longest_streak,
    first_dynasty_start,

    -- dominance score
    (longest_streak * 2 + total_dynasty_years) AS dynasty_strength_score

FROM aggregated
ORDER BY dynasty_strength_score DESC
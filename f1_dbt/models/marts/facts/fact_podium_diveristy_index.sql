WITH podium_finishers AS (

    SELECT
        season,
        driver_id,
        race_id
    FROM {{ ref('fact_results') }}
    WHERE finish_position IN (1, 2, 3)

),

season_races AS (

    SELECT
        season,
        COUNT(DISTINCT race_id) AS total_races
    FROM {{ ref('fact_results') }}
    GROUP BY season

),

podium_diversity AS (

    SELECT
        season,
        COUNT(DISTINCT driver_id) AS distinct_podium_drivers
    FROM podium_finishers
    GROUP BY season

)

SELECT
    p.season,
    p.distinct_podium_drivers,
    s.total_races,

    ROUND(
        (
            p.distinct_podium_drivers::numeric
            / (s.total_races * 3)
        ),
        3
    ) AS podium_diversity_ratio

FROM podium_diversity p
JOIN season_races s USING (season)

ORDER BY p.season
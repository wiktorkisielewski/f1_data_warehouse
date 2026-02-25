WITH driver_totals AS (

    SELECT
        season,
        driver_id,
        SUM(points) AS total_points
    FROM {{ ref('fact_results_enriched') }}
    GROUP BY season, driver_id

),

ranked_drivers AS (

    SELECT
        season,
        driver_id,
        total_points,
        RANK() OVER (
            PARTITION BY season
            ORDER BY total_points DESC
        ) AS driver_rank
    FROM driver_totals

)

SELECT
    fr.season,
    fr.race_round,
    fr.driver_id,
    fr.driver_name,
    fr.constructor_id,
    fr.constructor_name,
    rd.driver_rank,

    SUM(fr.points) OVER (
        PARTITION BY fr.season, fr.driver_id
        ORDER BY fr.race_round
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_points

FROM {{ ref('fact_results_enriched') }} fr
JOIN ranked_drivers rd
    ON fr.season = rd.season
   AND fr.driver_id = rd.driver_id

ORDER BY fr.season, fr.race_round
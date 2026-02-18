WITH season_races AS (

    SELECT
        season,
        COUNT(DISTINCT race_id) AS total_races
    FROM {{ ref('fact_results') }}
    GROUP BY season

),

driver_season_wins AS (

    SELECT
        season,
        driver_id,
        COUNT(*) FILTER (WHERE finish_position = 1) AS wins
    FROM {{ ref('fact_results') }}
    GROUP BY season, driver_id

),

driver_ranked AS (

    SELECT
        season,
        driver_id,
        wins,
        ROW_NUMBER() OVER (
            PARTITION BY season
            ORDER BY wins DESC
        ) AS rnk
    FROM driver_season_wins

),

champion_wins AS (

    SELECT
        d.season,
        d.driver_id,
        d.wins AS top_driver_wins,
        s.total_races,

        ROUND(
            (d.wins::numeric / s.total_races),
            3
        ) AS win_share

    FROM driver_ranked d
    JOIN season_races s USING (season)
    WHERE d.rnk = 1

)

SELECT
    cw.season,
    cw.driver_id,
    dr.first_name || ' ' || dr.last_name AS driver_name,
    cw.top_driver_wins,
    cw.total_races,
    cw.win_share,

    ROUND(
        AVG(cw.win_share) OVER (
            ORDER BY cw.season
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric,
        3
    ) AS win_share_rolling_3yr

FROM champion_wins cw
LEFT JOIN {{ ref('dim_drivers') }} dr
    ON cw.driver_id = dr.driver_id

ORDER BY cw.season
WITH driver_season_stats AS (

    SELECT
        season,
        driver_id,
        COUNT(*) FILTER (WHERE finish_position = 1) AS wins,
        COUNT(*) AS races
    FROM {{ ref('fact_results') }}
    GROUP BY season, driver_id

),

driver_win_share AS (

    SELECT
        season,
        driver_id,
        wins,
        races,
        CASE 
            WHEN races > 0 THEN wins::numeric / races
            ELSE 0
        END AS win_share
    FROM driver_season_stats

),

career_aggregation AS (

    SELECT
        driver_id,
        COUNT(DISTINCT season) AS career_length,
        SUM(wins) AS total_wins,
        MAX(win_share) AS peak_win_share
    FROM driver_win_share
    GROUP BY driver_id

),

driver_titles AS (

    SELECT
        driver_id,
        COUNT(*) AS total_titles
    FROM {{ ref('fact_championships') }}
    GROUP BY driver_id

)

SELECT
    c.driver_id,
    dr.first_name || ' ' || dr.last_name AS driver_name,
    c.career_length,
    c.total_wins,
    ROUND(c.peak_win_share::numeric, 4) AS peak_win_share,
    COALESCE(t.total_titles, 0) AS total_titles

FROM career_aggregation c

LEFT JOIN driver_titles t
    ON c.driver_id = t.driver_id

LEFT JOIN {{ ref('dim_drivers') }} dr
    ON c.driver_id = dr.driver_id

WHERE c.total_wins > 0

ORDER BY c.total_wins DESC
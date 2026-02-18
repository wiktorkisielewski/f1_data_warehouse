WITH season_points AS (

    SELECT
        season,
        driver_id,
        SUM(points) AS total_points
    FROM {{ ref('fact_results') }}
    GROUP BY season, driver_id

),

ranked AS (

    SELECT
        season,
        driver_id,
        total_points,
        RANK() OVER (
            PARTITION BY season
            ORDER BY total_points DESC
        ) AS pos
    FROM season_points

),

season_totals AS (

    SELECT
        season,
        SUM(total_points) AS total_points_awarded
    FROM season_points
    GROUP BY season

),

champion_vs_p2 AS (

    SELECT
        r1.season,

        r1.driver_id AS champion_driver_id,
        r1.total_points AS champion_points,

        r2.driver_id AS second_driver_id,
        r2.total_points AS second_points,

        st.total_points_awarded

    FROM ranked r1
    JOIN ranked r2
        ON r1.season = r2.season
       AND r2.pos = 2
    JOIN season_totals st
        ON r1.season = st.season
    WHERE r1.pos = 1

)

SELECT
    cvp.season,

    cvp.champion_driver_id,
    dc1.first_name || ' ' || dc1.last_name AS champion_name,
    cvp.champion_points,

    cvp.second_driver_id,
    dc2.first_name || ' ' || dc2.last_name AS second_name,
    cvp.second_points,

    (cvp.champion_points - cvp.second_points) AS raw_margin,

    ROUND(
        (
            (cvp.champion_points - cvp.second_points)::numeric
            / cvp.total_points_awarded::numeric
        ),
        4
    ) AS normalized_margin

FROM champion_vs_p2 cvp

LEFT JOIN {{ ref('dim_drivers') }} dc1
    ON cvp.champion_driver_id = dc1.driver_id

LEFT JOIN {{ ref('dim_drivers') }} dc2
    ON cvp.second_driver_id = dc2.driver_id

ORDER BY cvp.season
WITH driver_agg AS (

    SELECT
        season,
        driver_id,
        COUNT(DISTINCT race_id) AS races_entered,
        SUM(CASE WHEN status <> 'Finished' THEN 1 ELSE 0 END) AS dnfs,
        SUM(points) AS total_points
    FROM {{ ref('fact_results') }}
    GROUP BY season, driver_id
    HAVING COUNT(DISTINCT race_id) >= 10

),

scored AS (

    SELECT
        season,
        driver_id,
        races_entered,
        dnfs,
        total_points,

        ROUND(
            (
                (dnfs::numeric / races_entered) * 10
                -
                (total_points::numeric / races_entered)
            ),
            2
        ) AS fumble_score

    FROM driver_agg

)

SELECT
    s.season,
    s.driver_id,
    d.first_name || ' ' || d.last_name AS driver_name,
    s.races_entered,
    s.dnfs,
    s.total_points,
    s.fumble_score

FROM scored s
LEFT JOIN {{ ref('dim_drivers') }} d
    ON s.driver_id = d.driver_id

WHERE s.fumble_score > 0
ORDER BY s.season, s.fumble_score DESC
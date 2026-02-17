WITH championships AS (
    SELECT
        season,
        driver_id
    FROM {{ ref('fact_championships') }}
),

-- Detect streak groups
streaks AS (
    SELECT
        driver_id,
        season,
        season - ROW_NUMBER() OVER (
            PARTITION BY driver_id
            ORDER BY season
        ) AS streak_group
    FROM championships
),

-- Aggregate streaks
streak_summary AS (
    SELECT
        driver_id,
        MIN(season) AS dynasty_start,
        MAX(season) AS dynasty_end,
        COUNT(*) AS streak_length
    FROM streaks
    GROUP BY driver_id, streak_group
    HAVING COUNT(*) >= 2   -- only 2+ consecutive seasons count as dynasty
),

-- Final driver-level summary
driver_summary AS (
    SELECT
        driver_id,
        COUNT(*) AS number_of_dynasties,
        SUM(streak_length) AS total_dynasty_years,
        MAX(streak_length) AS longest_streak,
        MIN(dynasty_start) AS first_dynasty_start,
        MAX(dynasty_end) AS last_dynasty_end
    FROM streak_summary
    GROUP BY driver_id
)

SELECT
    ds.driver_id,
    dr.first_name || ' ' || dr.last_name AS driver_name,
    ds.number_of_dynasties,
    ds.total_dynasty_years,
    ds.longest_streak,
    ds.first_dynasty_start,
    ds.last_dynasty_end,

    -- Scaled bubble size
    POWER(ds.longest_streak, 2) * 25 AS longest_streak_scaled,

    -- Midpoint for positioning
    FLOOR((ds.first_dynasty_start + ds.last_dynasty_end) / 2.0) AS dynasty_midpoint,

    -- Era grouping
    CASE
        WHEN FLOOR((ds.first_dynasty_start + ds.last_dynasty_end) / 2.0) < 1970 THEN 'Classic Era'
        WHEN FLOOR((ds.first_dynasty_start + ds.last_dynasty_end) / 2.0) < 1990 THEN 'Turbo Era'
        WHEN FLOOR((ds.first_dynasty_start + ds.last_dynasty_end) / 2.0) < 2010 THEN 'Modern Era'
        ELSE 'Hybrid Era'
    END AS era_group,

    -- Strength metric
    (ds.total_dynasty_years * 2 + ds.longest_streak) AS dynasty_strength_score

FROM driver_summary ds
LEFT JOIN {{ ref('dim_drivers') }} dr
    ON ds.driver_id = dr.driver_id

ORDER BY dynasty_strength_score DESC
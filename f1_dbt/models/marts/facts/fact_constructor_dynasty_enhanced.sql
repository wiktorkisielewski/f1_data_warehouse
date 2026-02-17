WITH constructor_champions AS (

    SELECT
        season,
        constructor_id
    FROM {{ ref('fact_championships') }}

),

-- Detect consecutive streak groups
streak_base AS (

    SELECT
        constructor_id,
        season,
        season - ROW_NUMBER() OVER (
            PARTITION BY constructor_id
            ORDER BY season
        ) AS grp
    FROM constructor_champions

),

-- Aggregate streaks
streaks AS (

    SELECT
        constructor_id,
        MIN(season) AS dynasty_start,
        MAX(season) AS dynasty_end,
        COUNT(*) AS streak_length
    FROM streak_base
    GROUP BY constructor_id, grp

),

-- Real dynasties (3+ consecutive titles)
filtered_dynasties AS (

    SELECT *
    FROM streaks
    WHERE streak_length >= 3

),

-- Constructor-level summary
constructor_summary AS (

    SELECT
        constructor_id,
        COUNT(*) AS number_of_dynasties,
        SUM(streak_length) AS total_dynasty_years,
        MAX(streak_length) AS longest_streak,
        MIN(dynasty_start) AS first_dynasty_start,
        MIN(dynasty_end) AS first_dynasty_end
    FROM filtered_dynasties
    GROUP BY constructor_id

),

-- Weighted dominance center
weighted_center AS (

    SELECT
        constructor_id,
        SUM(
            ((dynasty_start + dynasty_end) / 2.0) * streak_length
        )
        /
        SUM(streak_length)
        AS weighted_dynasty_midpoint
    FROM filtered_dynasties
    GROUP BY constructor_id

)

-- Final Output
SELECT
    cs.constructor_id,
    dc.constructor_name,
    cs.number_of_dynasties,
    cs.total_dynasty_years,
    cs.longest_streak,
    cs.first_dynasty_start,
    cs.first_dynasty_end,

    wc.weighted_dynasty_midpoint AS dynasty_midpoint,

    -- Aggressive bubble scaling
    POWER(cs.longest_streak, 2) * 35
        AS longest_streak_scaled,

    -- Era classification
    CASE
        WHEN wc.weighted_dynasty_midpoint < 1970 THEN 'Classic Era'
        WHEN wc.weighted_dynasty_midpoint < 1990 THEN 'Turbo Era'
        WHEN wc.weighted_dynasty_midpoint < 2010 THEN 'Modern Era'
        ELSE 'Hybrid Era'
    END AS era_group

FROM constructor_summary cs
JOIN weighted_center wc
    ON cs.constructor_id = wc.constructor_id

LEFT JOIN {{ ref('dim_constructors') }} dc
    ON cs.constructor_id = dc.constructor_id

ORDER BY dynasty_midpoint
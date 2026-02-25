WITH constructor_titles AS (

    SELECT
        season,
        constructor_id
    FROM {{ ref('fact_championships') }}

),

title_counts AS (

    SELECT
        constructor_id,
        COUNT(*) AS total_titles
    FROM constructor_titles
    GROUP BY constructor_id

),

streak_base AS (

    SELECT
        constructor_id,
        season,
        season - ROW_NUMBER() OVER (
            PARTITION BY constructor_id
            ORDER BY season
        ) AS grp
    FROM constructor_titles

),

streaks AS (

    SELECT
        constructor_id,
        MIN(season) AS dynasty_start,
        MAX(season) AS dynasty_end,
        COUNT(*) AS streak_length
    FROM streak_base
    GROUP BY constructor_id, grp

),

streak_summary AS (

    SELECT
        constructor_id,
        COUNT(*) AS number_of_dynasties,
        MAX(streak_length) AS longest_streak
    FROM streaks
    GROUP BY constructor_id

),

gap_base AS (

    SELECT
        constructor_id,
        season,
        season - LAG(season) OVER (
            PARTITION BY constructor_id
            ORDER BY season
        ) AS gap
    FROM constructor_titles

),

gap_summary AS (

    SELECT
        constructor_id,
        AVG(gap) AS avg_gap_between_titles,
        MAX(gap) AS longest_gap
    FROM gap_base
    WHERE gap IS NOT NULL
    GROUP BY constructor_id

),

final AS (

    SELECT
        tc.constructor_id,
        dc.constructor_name,
        tc.total_titles,
        ss.number_of_dynasties,
        ss.longest_streak,
        gs.avg_gap_between_titles,
        gs.longest_gap,

        ROUND(
            (
                (
                    POWER(ss.longest_streak, 2)::numeric
                    / NULLIF(gs.avg_gap_between_titles::numeric, 0)
                )
                * LN(tc.total_titles + 1)::numeric
            )::numeric,
            2
        ) AS dynasty_stability_index

    FROM title_counts tc
    LEFT JOIN streak_summary ss USING (constructor_id)
    LEFT JOIN gap_summary gs USING (constructor_id)
    LEFT JOIN {{ ref('dim_constructors') }} dc
        ON tc.constructor_id = dc.constructor_id

)

SELECT *
FROM final
ORDER BY dynasty_stability_index DESC
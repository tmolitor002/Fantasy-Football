-- Purpose: Calculate the number of fantasy football points scored per play. End goal is to use this as the target score for an ML model (xg boost?)
WITH src AS (
    SELECT
        _play_id
        , first_down_rush
        , two_point_attempt
        , fumble
        , fumble_lost
        , rush_attempt
        , rushing_yards
        , yards_gained
        , touchdown
        , CASE
            WHEN rushing_yards >= ydstogo THEN 1
            ELSE 0
            END AS rushing_first_down
        , CASE
            WHEN rushing_yards >= 40 AND rush_touchdown = 0 THEN 1
            ELSE 0
            END AS forty_yd_plus_rush
        , CASE
            WHEN rushing_yards >= 40 and rush_touchdown = 1 THEN 1
            ELSE 0
            END AS forty_yd_plus_rush_td
    FROM {{ ref('int_play_by_play_consolidated_rushing') }}
)

, scoring AS (
    SELECT
        -- league_id
        fractional_points               AS scoring_fractional_points
        , fumble                        AS scoring_fumble
        , fumble_lost                   AS scoring_fumble_lost
        , rushing_touchdown             AS scoring_rushing_touchdown
        , rushing_attempt               AS scoring_rushing_attempt
        , rushing_yards_pp              AS scoring_rushing_yards_pp
        , rushing_40_yd_plus_rush       AS scoring_40_yd_plus_rush
        , rushing_40_yd_plus_touchdown  AS scoring_rushing_40_yd_plus_touchdown
        , rushing_first_down            AS scoring_rushing_first_down
    FROM {{ ref('stg_league_scoring') }}
)

, join_scoring AS (
    SELECT
        a.*
        , b.*
    FROM src a
    LEFT JOIN scoring b
        ON 1=1
)

, category_scoring AS (
    SELECT
        _play_id
        , CAST((touchdown * scoring_rushing_touchdown) AS DECIMAL)                          AS rushing_touchdown
        , CAST((rush_attempt * scoring_rushing_attempt) AS DECIMAL)                         AS rushing_attempt
        , CASE
            WHEN scoring_fractional_points = true THEN (CAST(rushing_yards AS DECIMAL) / CAST(scoring_rushing_yards_pp AS DECIMAL))
            ELSE (CAST(rushing_yards AS DECIMAL) / CAST(scoring_rushing_yards_pp AS DECIMAL)) -- fractional scoring is stupid, and only really matters at the end of a game
            END                                                             AS rushing_yards
        , CAST((forty_yd_plus_rush * scoring_40_yd_plus_rush) AS DECIMAL)                   AS explosive_rush
        , CAST((forty_yd_plus_rush_td * scoring_rushing_40_yd_plus_touchdown) AS DECIMAL)   AS explosive_rushing_touchdown
        , CAST((rushing_first_down * scoring_rushing_first_down) AS DECIMAL)                AS first_down
        , CAST((fumble_lost * scoring_fumble_lost) AS DECIMAL)                              AS fumble_lost
        , CAST((fumble * scoring_fumble) AS DECIMAL)                                        AS fumble
    FROM join_scoring
)

, total_fantasy_points AS (
    SELECT
        _play_id
        , (
            rushing_touchdown
            + rushing_attempt
            + rushing_yards
            + explosive_rush
            + explosive_rushing_touchdown
            + first_down
            + fumble_lost
            + fumble
        ) AS fantasy_points
    FROM category_scoring
)

, final AS (
    SELECT
        COALESCE(b.fantasy_points, 0) AS fantasy_points
        , a.*
    FROM {{ ref('int_play_by_play_consolidated_rushing') }} a
    LEFT JOIN total_fantasy_points b
        ON a._play_id = b._play_id
)

SELECT *
FROM final
ORDER BY season, week, game_date, game_id, play_id ASC

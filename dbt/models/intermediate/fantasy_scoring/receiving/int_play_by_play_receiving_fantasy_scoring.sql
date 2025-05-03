-- Purpose: Calculate the number of fantasy football points scored per play. End goal is to use this as the target score for an ML model (xg boost?)
WITH src AS (
    SELECT
        _play_id
        , ydstogo
        , air_yards
        , yards_after_catch
        , yards_gained -- not necessary?
        , CAST(fumble_lost AS DECIMAL)      AS fumble_lost
        , CAST(fumble AS DECIMAL)           AS fumble
        , CAST(complete_pass AS DECIMAL)    AS complete_pass
        --, touchdown
        , CAST(pass_touchdown AS DECIMAL)   AS pass_touchdown
        , passing_yards -- should be same as below
        , CAST(receiving_yards AS DECIMAL)  AS receiving_yards -- use this, yards gained includes sacks and incomplete passes
        , CASE
            WHEN receiving_yards >= ydstogo THEN CAST(1 AS DECIMAL)
            ELSE CAST(0 AS DECIMAL)
            END AS receiving_first_down
        , CASE
            WHEN receiving_yards >= 40 THEN CAST(1 AS DECIMAL)
            ELSE CAST(0 AS DECIMAL)
            END AS forty_yd_plus_reception
        , CASE WHEN receiving_yards >= 40 and pass_touchdown = 1 THEN CAST(1 AS DECIMAL)
            ELSE CAST(0 AS DECIMAL)
            END AS forty_yd_plus_reception_td
    FROM {{ ref('int_play_by_play_consolidated_receiving') }}
    WHERE receiver_player_id IS NOT NULL
)

, scoring AS (
    SELECT
        fractional_points
        , CAST(fumble_lost AS DECIMAL)                      AS scoring_fumble_lost
        , CAST(fumble AS DECIMAL)                           AS scoring_fumble
        , CAST(receiving_touchdown AS DECIMAL)              AS scoring_receiving_touchdown
        , CAST(receiving_reception AS DECIMAL)              AS scoring_receiving_reception
        , CAST(receiving_yards_pp AS DECIMAL)               AS scoring_receiving_yards_pp
        , CAST(receiving_first_down AS DECIMAL)             AS scoring_receiving_first_down
        , CAST(receiving_40_yd_plus_reception AS DECIMAL)   AS scoring_receiving_40_yd_plus_reception
        , CAST(receiving_40_yd_plus_touchdown AS DECIMAL)   AS scoring_receiving_40_yd_plus_touchdown
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
        , (fumble_lost * scoring_fumble_lost)                                   AS receiving_fumble_lost
        , (fumble * scoring_fumble)                                             AS receiving_fumble
        , (pass_touchdown * scoring_receiving_touchdown)                        AS receiving_touchdown
        , (complete_pass * scoring_receiving_reception)                         AS receiving_reception
        , (receiving_yards / scoring_receiving_yards_pp)                        AS receiving_yards -- non-fractional scoring really only matters at a game level
        , (receiving_first_down * scoring_receiving_first_down)                 AS receiving_first_down
        , (forty_yd_plus_reception * scoring_receiving_40_yd_plus_reception)    AS receiving_explosive_reception
        , (forty_yd_plus_reception_td * scoring_receiving_40_yd_plus_touchdown) AS receiving_explosive_touchdown
    FROM join_scoring
)

, total_fantasy_points AS (
    SELECT
        _play_id
        , (
            receiving_fumble_lost
            + receiving_fumble
            + receiving_touchdown
            + receiving_reception
            + receiving_yards
            + receiving_first_down
            + receiving_explosive_reception
            + receiving_explosive_touchdown
        ) AS fantasy_points
    FROM category_scoring
)

, final AS (
    SELECT
        COALESCE(b.fantasy_points, 0) AS fantasy_points
        , a.*
    FROM {{ ref('int_play_by_play_consolidated_receiving') }} a
    LEFT JOIN total_fantasy_points b
        ON a._play_id = b._play_id
)

SELECT *
FROM final
ORDER BY season, week, game_date, game_id, play_id ASC

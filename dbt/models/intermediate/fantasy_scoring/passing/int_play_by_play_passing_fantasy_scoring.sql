-- Purpose: Calculate the number of fantasy football points scored per play. End goal is to use this as the target score for an ML model (xg boost?)
WITH src AS (
    SELECT
        _play_id
        , ydstogo
        , CAST(pass_attempt AS DECIMAL)                 AS pass_attempt
        , CAST(incomplete_pass AS DECIMAL)              AS incomplete_pass
        , CAST(complete_pass AS DECIMAL)                AS complete_pass
        , CAST(sack AS DECIMAL)                         AS sack
        , CAST(interception AS DECIMAL)                 AS interception
        , CAST(pass_touchdown AS DECIMAL)               AS pass_touchdown
        , CAST(COALESCE(passing_yards, 0) AS DECIMAL)   AS passing_yards -- used for passing yards per point
        , CAST(yards_gained AS DECIMAL)                 AS yards_gained -- used for negative rushing when sacked
        , CAST(fumble AS DECIMAL)                       AS fumble
        , CAST(fumble_lost AS DECIMAL)                  AS fumble_lost
        , CASE
            WHEN interception = 1 and touchdown = 1 THEN 1.0
            ELSE 0.0
            END AS pick_six
        -- fumble_lost, interception + touchdown both = 1
        , CASE
            WHEN passing_yards >= 40 THEN 1.0
            ELSE 0.0
            END AS explosive_pass
        , CASE
            WHEN passing_yards >= 40 AND pass_touchdown = 1 THEN 1.0
            ELSE 0.0
            END AS explosive_pass_td
        , CASE
            WHEN passing_yards >= ydstogo THEN 1.0
            ELSE 0.0
            END AS passing_first_down
    FROM {{ ref('int_play_by_play_consolidated_passing') }}
)

, scoring AS (
    SELECT
        fractional_points
        , CAST(passing_touchdown AS DECIMAL)                AS scoring_passing_touchdown
        , CAST(passing_interception AS DECIMAL)             AS scoring_passing_interception
        , CAST(passing_attempts AS DECIMAL)                 AS scoring_passing_attempts
        , CAST(passing_completion AS DECIMAL)               AS scoring_passing_completion
        , CAST(passing_incomplete AS DECIMAL)               AS scoring_passing_incomplete
        , CAST(passing_sack AS DECIMAL)                     AS scoring_passing_sack
        , CAST(passing_yards_pp AS DECIMAL)                 AS scoring_passing_yards_pp
        , CAST(passing_pick_six AS DECIMAL)                 AS scoring_passing_pick_six
        , CAST(passing_first_down AS DECIMAL)               AS scoring_passing_first_down
        , CAST(passing_40_yd_plus_completion AS DECIMAL)    AS scoring_passing_40_yd_plus_completion
        , CAST(passing_40_yd_plus_touchdown AS DECIMAL)     AS scoring_passing_40_yd_plus_touchdown
        , CAST(fumble AS DECIMAL)                           AS scoring_fumble
        , CAST(fumble_lost AS DECIMAL)                      AS scoring_fumble_lost
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
        , (pass_touchdown * scoring_passing_touchdown)                  AS pass_touchdown
        , (interception * scoring_passing_interception)                 AS interception
        , (pass_attempt * scoring_passing_attempts)                     AS pass_attempt
        , (complete_pass * scoring_passing_completion)                  AS complete_pass
        , (incomplete_pass * scoring_passing_incomplete)                AS incomplete_pass
        , (sack * scoring_passing_sack)                                 AS sack
        , (passing_yards / scoring_passing_yards_pp)                    AS passing_yards
        , (pick_six * scoring_passing_pick_six)                         AS pick_six
        , (explosive_pass * scoring_passing_40_yd_plus_completion)      AS explosive_pass
        , (explosive_pass_td * scoring_passing_40_yd_plus_touchdown)    AS explosive_pass_td
        , (passing_first_down * scoring_passing_first_down)             AS passing_first_down
        , (fumble * scoring_fumble)                                     AS fumble
        , (fumble_lost * scoring_fumble_lost)                           AS fumble_lost
    FROM join_scoring
)

, total_fantasy_points AS (
    SELECT
        _play_id
        , (
            pass_touchdown
            + interception
            + pass_attempt
            + complete_pass
            + incomplete_pass
            + sack
            + passing_yards
            + pick_six
            + explosive_pass
            + passing_first_down
            + fumble
            + fumble_lost
        ) AS fantasy_points
    FROM category_scoring
)

, final AS (
    SELECT
        COALESCE(b.fantasy_points, 0) AS fantasy_points
        , a.*
    FROM {{ ref('int_play_by_play_consolidated_passing') }} a
    LEFT JOIN total_fantasy_points b
        ON a._play_id = b._play_id
)

SELECT *
FROM final
ORDER BY season, week, game_date, game_id, play_id ASC

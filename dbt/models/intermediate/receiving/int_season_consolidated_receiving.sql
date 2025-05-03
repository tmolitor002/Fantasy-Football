WITH receiving AS (
    SELECT *
    FROM {{ ref('int_play_by_play_consolidated_receiving') }}
    WHERE receiver_player_id IS NOT NULL
)

, players AS (
    SELECT
        gsis_id                     AS player_id
        , display_name
        , "position"
    FROM {{ ref('stg_players') }}
)

, season AS (
    SELECT
        -- Game info
        season
        , posteam
        , season_type
        -- Receiver info
        , receiver_player_id
        , SUM(pass_attempt)         AS total_targets
        , SUM(incomplete_pass)      AS total_incomplete_targets
        , SUM(complete_pass)        AS total_receptions
        , SUM(air_yards)            AS total_air_yards
        , SUM(yards_after_catch)    AS total_yards_after_catch
        , SUM(passing_yards)        AS total_passing_yards
        , SUM(receiving_yards)      AS total_receiving_yards --should be same as above
        , SUM(pass_touchdown)       AS total_receiving_touchdowns
        , SUM(goal_to_go)           AS total_goal_to_go_reception_attempts
        , SUM(shotgun)              AS total_shotgun_reception_attempts
        , SUM(no_huddle)            AS total_no_huddle_reception_attempts
        , SUM(qb_dropback)          AS total_qb_dropback_reception_attempts
        , SUM(first_down_pass)      AS total_first_down_pass_attempts
        , SUM(two_point_attempt)    AS total_two_point_attempts
        , SUM(qb_scramble)          AS total_qb_scrambles
        , SUM(qb_hit)               AS total_qb_hits
        , SUM(fumble)               AS total_fumbles
        , SUM(touchdown)            AS total_touchdowns
        , SUM(yards_gained)         AS total_yards_gained
        , AVG(down)                 AS avg_down
        , AVG(ydstogo)              AS avg_ydstogo
        , AVG(air_yards)            AS avg_air_yards
        , AVG(yards_after_catch)    AS avg_yards_after_catch
        , AVG(passing_yards)        AS avg_passing_yards
        , AVG(receiving_yards)      AS avg_receiving_yards
        , AVG(yards_gained)         AS avg_yards_gained
    FROM receiving
    GROUP BY
        season
        , posteam
        , season_type
        , receiver_player_id
)

, join_receiver_name AS (
    SELECT
        a.season
        , a.posteam
        , a.season_type
        , a.receiver_player_id
        , b.display_name
        , b."position"
        , COALESCE(a.total_targets, 0) AS total_targets
        , COALESCE(a.total_incomplete_targets, 0) AS total_incomplete_targets
        , COALESCE(a.total_receptions, 0) AS total_receptions
        , (CAST(a.total_receptions AS DECIMAL) / CAST(a.total_targets AS DECIMAL)) AS catch_rate
        , COALESCE(a.total_air_yards, 0) AS total_air_yards
        , COALESCE(a.total_yards_after_catch, 0) AS total_yards_after_catch
        , COALESCE(a.total_passing_yards, 0) AS total_passing_yards
        , COALESCE(a.total_receiving_yards, 0) AS total_receiving_yards --should be same as above
        , COALESCE(a.total_receiving_touchdowns, 0) AS total_receiving_touchdowns
        , COALESCE(a.total_goal_to_go_reception_attempts, 0) AS total_goal_to_go_reception_attempts
        , COALESCE(a.total_shotgun_reception_attempts, 0) AS total_shotgun_reception_attempts
        , COALESCE(a.total_no_huddle_reception_attempts, 0) AS total_no_huddle_reception_attempts
        , COALESCE(a.total_qb_dropback_reception_attempts, 0) AS total_qb_dropback_reception_attempts
        , COALESCE(a.total_first_down_pass_attempts, 0) AS total_first_down_pass_attempts
        , COALESCE(a.total_two_point_attempts, 0) AS total_two_point_attempts
        , COALESCE(a.total_qb_scrambles, 0) AS total_qb_scrambles
        , COALESCE(a.total_qb_hits, 0) AS total_qb_hits
        , COALESCE(a.total_fumbles, 0) AS total_fumbles
        , COALESCE(a.total_touchdowns, 0) AS total_touchdowns
        , COALESCE(a.total_yards_gained, 0) AS total_yards_gained
        , COALESCE(a.avg_down, 0) AS avg_down
        , COALESCE(a.avg_ydstogo, 0) AS avg_ydstogo
        , COALESCE(a.avg_air_yards, 0) AS avg_air_yards
        , COALESCE(a.avg_yards_after_catch, 0) AS avg_yards_after_catch
        , COALESCE(a.avg_passing_yards, 0) AS avg_passing_yards
        , COALESCE(a.avg_receiving_yards, 0) AS avg_receiving_yards
        , COALESCE(a.avg_yards_gained, 0) AS avg_yards_gained
    FROM season a
    LEFT JOIN players b
        ON a.receiver_player_id = b.player_id
)

, final AS (
    SELECT *
    FROM join_receiver_name
)

SELECT *
FROM final
ORDER BY season ASC, total_targets DESC

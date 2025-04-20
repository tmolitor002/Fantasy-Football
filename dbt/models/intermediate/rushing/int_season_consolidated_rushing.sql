WITH rushing AS (
    SELECT *
    FROM {{ ref('int_play_by_play_consolidated_rushing') }}
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
        -- Rusher info
        , rusher_player_id
        , COUNT(DISTINCT game_id)   AS games_played
        , SUM(goal_to_go)           AS total_goal_to_go_rushes
        , SUM(shotgun)              AS total_shotgun_rushes
        , SUM(no_huddle)            AS total_no_huddle_rushes
        , SUM(qb_dropback)          AS total_qb_dropback_rushes
        , SUM(qb_kneel)             AS total_qb_kneels
        , SUM(first_down_rush)      AS total_first_down_rushes
        , SUM(two_point_attempt)    AS total_two_point_attempts
        , SUM(fumble)               AS total_fumbles
        , SUM(qb_scramble)          AS total_qb_scrambles
        , SUM(tackled_for_loss)     AS total_tackled_for_loss
        , SUM(rush_attempt)         AS total_rush_attempts
        , SUM(rush_touchdown)       AS total_rush_touchdowns
        , SUM(touchdown)            AS total_touchdowns
        , SUM(rushing_yards)        AS total_rushing_yards
        , SUM(yards_gained)         AS total_yards_gained
        , AVG(down)                 AS avg_down_by_attempt
        , AVG(ydstogo)              AS avg_ydstogo_by_attempt
        , AVG(rushing_yards)        AS avg_rushing_yards_by_attempt
        , AVG(yards_gained)         AS avg_yards_gained_by_attempt
    FROM rushing
    GROUP BY
        season
        , posteam
        , season_type
        , rusher_player_id
)

, join_rusher_name AS (
    SELECT
        a.season
        , a.posteam
        , a.rusher_player_id
        , b.display_name
        , b."position"
        , a.season_type
        , a.games_played
        , a.total_rush_attempts
        , a.total_rushing_yards
        , a.total_rush_touchdowns
        , a.total_fumbles
        , a.total_yards_gained
        , a.total_goal_to_go_rushes
        , a.total_shotgun_rushes
        , a.total_no_huddle_rushes
        , a.total_qb_dropback_rushes
        , a.total_qb_kneels
        , a.total_first_down_rushes
        , a.total_two_point_attempts
        , a.total_qb_scrambles
        , a.total_tackled_for_loss
        , a.total_touchdowns
        , a.avg_down_by_attempt
        , a.avg_ydstogo_by_attempt
        , a.avg_rushing_yards_by_attempt
        , a.avg_yards_gained_by_attempt
    FROM season a
    LEFT JOIN players b
        ON a.rusher_player_id = b.player_id
)

, final AS (
    SELECT *
    FROM join_rusher_name
)

SELECT *
FROM final
ORDER BY season ASC, total_rush_attempts DESC

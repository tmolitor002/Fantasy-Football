/*
Thoughts for additional parameters
- The actual player
- Player age on game day
- Starting (or main) Quarterback (had the most pass attempts in that game)
- defense team
- Offensive line
- home vs. away
- formation/personnel?
- depth chart? (percentage of usage by team, snaps played, target share)
-- window functions
- pos coach tendencies (rush vs. pass percentage over last x years)
- fourth down likelihood (detroit goes for it often)

*/

WITH src AS (
    SELECT
        fantasy_points
        , _play_id
        , rusher_player_id -- do not include in ML
        -- game info
        , season
        , week
        , TO_CHAR(game_date::DATE, 'day') AS day_of_week
        , surface
        , season_type
        , pos_coach
        , def_coach
        -- pre-huddle
        , yardline_100
        , drive
        , down
        , CASE
            WHEN goal_to_go = 1 THEN TRUE
            ELSE FALSE
            END AS goal_to_go
        , ydstogo
        , posteam_timeouts_remaining
        , defteam_timeouts_remaining
        , posteam_score
        , defteam_score
        , score_differential
        -- play-design
        , shotgun
        , no_huddle
        , qb_kneel
        , run_location
        , COALESCE(run_gap, 'middle') AS run_gap
        -- pre-snap
        , game_seconds_remaining
        -- play-execution
        , qb_dropback
    FROM {{ ref('int_play_by_play_rushing_fantasy_scoring') }}
)

, positions AS (
    SELECT
        gsis_id AS rusher_player_id
        , "position" AS pos
    FROM {{ ref('stg_players') }}
)

, join_position AS (
    SELECT
        b.pos
        , a.*
    FROM src a
    LEFT JOIN positions b
        ON a.rusher_player_id = b.rusher_player_id
)

, final AS (
    SELECT
        fantasy_points
        , _play_id
        , rusher_player_id
        , season
        , week
        , day_of_week
        , surface
        , season_type
        , pos_coach
        , def_coach
        , yardline_100
        , drive
        , down
        , goal_to_go
        , posteam_timeouts_remaining
        , defteam_timeouts_remaining
        , posteam_score
        , defteam_score
        , score_differential
        , pos
        , shotgun
        , no_huddle
        , qb_kneel
        , run_location
        , run_gap
        , game_seconds_remaining
        , qb_dropback
    FROM join_position
)

SELECT *
FROM final

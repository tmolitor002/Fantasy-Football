WITH src AS (
    SELECT
        fantasy_points
        , _play_id
        , receiver_player_id
        -- game info
        , season
        , week
        , TO_CHAR(game_date::DATE, 'day') AS day_of_week
        , surface
        , roof
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
        , posteam_timeouts_remaining
        , defteam_timeouts_remaining
        , posteam_score
        , defteam_score
        , score_differential
        -- play_design
        , passer_player_id -- who is the quarterback
        , shotgun
        , no_huddle
        , qb_dropback
        , qb_spike
        -- pre-snap
        , game_seconds_remaining
        -- play-execution
        , pass_length -- unsure if these should be used
        , pass_location -- unsure if thse should be used
    FROM {{ ref('int_play_by_play_passing_fantasy_scoring') }}
)

, positions AS (
    SELECT
        gsis_id AS receiver_player_id
        , gsis_id AS passer_player_id
        , "position" AS pos
    FROM {{ ref('stg_players') }}
)

, join_position AS (
    SELECT
        b.pos AS receiver_pos
        , c.pos AS passer_pos
        , a.*
    FROM src a
    LEFT JOIN positions b
        ON a.receiver_player_id = b.receiver_player_id
    LEFT JOIN positions c
        ON a.passer_player_id = c.passer_player_id
)

, final AS (
    SELECT
        fantasy_points
        , _play_id
        , passer_player_id
        , passer_pos
        , receiver_player_id
        , receiver_pos
        , season
        , week
        , day_of_week
        , surface
        , roof
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
        , shotgun
        , no_huddle
        , qb_dropback
        , qb_spike
        , game_seconds_remaining
        , COALESCE(pass_length, 'unknown')      AS pass_length
        , COALESCE(pass_location, 'unknown')    AS pass_location
    FROM join_position
)

SELECT *
FROM final

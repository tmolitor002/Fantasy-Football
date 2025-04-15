WITH src_pass_plays AS ( -- Select necessary fields for pass plays
    SELECT
        -- Record identification
        _play_id
        , season
        , week
        , game_date
        , game_id
        , play_id
        -- Game information (link to in_games_consolidated?)
        , spread_line
        , total_line
        , div_game
        , roof
        , surface
        , home_coach
        , away_coach
        , stadium_id
            -- Post game information
        , away_score
        , home_score
        , result
        , total
        -- Pre-huddle information
        , yardline_100
        , drive
        , down
        , goal_to_go
        , ydstogo
        , posteam_timeouts_remaining
        , defteam_timeouts_remaining
        , posteam_score
        , defteam_score
        , score_differential -- check this
        , series
        , drive_game_clock_start -- could use combined with seconds remaining to calculate how long team has been on field?
        -- Play design/huddle
        , passer_player_id
        , play_type
        , play_type_nfl
        , shotgun
        , no_huddle
        , qb_dropback
        , qb_kneel
        , qb_spike
        , first_down_pass
        , two_point_attempt
        -- At snap
        , quarter_seconds_remaining
        , half_seconds_remaining
        , game_seconds_remaining
        -- Play execution
        , receiver_player_id
        , qb_scramble
        , pass_length
        , pass_location
        , air_yards
        , yards_after_catch
        , qb_hit
        , fumble
        -- Play result information
        , "desc"                        AS description
        , yards_gained
        , incomplete_pass
        , interception
        , pass_attempt
        , complete_pass
        , sack
        , touchdown
        , pass_touchdown
        , passing_yards
        , receiving_yards
        , interception_player_id        
    FROM {{ ref('int_play_by_play_consolidated') }}
)

SELECT *
FROM src_pass_plays
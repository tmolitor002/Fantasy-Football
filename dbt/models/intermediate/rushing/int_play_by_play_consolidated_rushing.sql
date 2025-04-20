WITH rushing_plays AS (
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
        , season_type
            -- Post game information
        , away_score
        , home_score
        , result
        , total
        -- Pre-huddle information
        , posteam
        , pos_coach
        , defteam
        , def_coach
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
        , play_type
        , play_type_nfl
        , shotgun
        , no_huddle
        , qb_dropback
        , qb_kneel
        , first_down_rush
        , two_point_attempt
        , lateral_rush
        -- At snap
        , quarter_seconds_remaining
        , half_seconds_remaining
        , game_seconds_remaining
        -- Play execution
        , run_location
        , run_gap
        , rusher_player_id
        , lateral_rusher_player_id
        , lateral_rusher_player_name
        , fumble
        , qb_scramble
        -- Play result information
        , "desc"                                    AS description
        , tackled_for_loss
        , rush_attempt
        , rush_touchdown
        , rushing_yards
        , lateral_rushing_yards
        , yards_gained
        , touchdown
    FROM {{ ref('int_play_by_play_consolidated') }}
    WHERE play_type = 'run'
)

SELECT *
FROM rushing_plays
ORDER BY season, week, game_date, game_id, play_id ASC

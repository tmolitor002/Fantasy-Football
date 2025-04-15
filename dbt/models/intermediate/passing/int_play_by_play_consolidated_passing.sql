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
        , CAST(spread_line AS DECIMAL)              AS spread_line
        , CAST(total_line AS DECIMAL)               AS total_line
        , CAST(div_game AS INT)                     AS div_game
        , roof
        , surface
        , home_coach
        , away_coach
        , stadium_id
            -- Post game information
        , CAST(away_score AS INT)                   AS away_score
        , CAST(home_score AS INT)                   AS home_score
        , CAST(result AS INT)                       AS result
        , CAST(total AS INT)                        AS total
        -- Pre-huddle information
        , CAST(yardline_100 AS INT)                 AS yardline_100
        , CAST(drive AS INT)                        AS drive
        , CAST(down AS INT)                         AS down
        , CAST(goal_to_go AS INT)                   AS goal_to_go
        , CAST(ydstogo AS INT)                      AS ydstogo
        , CAST(posteam_timeouts_remaining AS INT)   AS posteam_timeouts_remaining
        , CAST(defteam_timeouts_remaining AS INT)   AS defteam_timeouts_remaining
        , CAST(posteam_score AS INT)                AS posteam_score
        , CAST(defteam_score AS INT)                AS defteam_score
        , CAST(score_differential AS INT)           AS score_differential -- check this
        , CAST(series AS INT)                       AS series
        , drive_game_clock_start -- could use combined with seconds remaining to calculate how long team has been on field?
        -- Play design/huddle
        , passer_player_id
        , play_type
        , play_type_nfl
        , CAST(shotgun AS INT)                      AS shotgun
        , CAST(no_huddle AS INT)                    AS no_huddle
        , CAST(qb_dropback AS INT)                  AS qb_dropback
        , CAST(qb_kneel AS INT)                     AS qb_kneel
        , CAST(qb_spike AS INT)                     AS qb_spike
        , CAST(first_down_pass AS INT)              AS first_down_pass
        , CAST(two_point_attempt AS INT)            AS two_point_attempt
        -- At snap
        , CAST(quarter_seconds_remaining AS INT)    AS quarter_seconds_remaining
        , CAST(half_seconds_remaining AS INT)       AS half_seconds_remaining
        , CAST(game_seconds_remaining AS INT)       AS game_seconds_remaining
        -- Play execution
        , receiver_player_id
        , CAST(qb_scramble AS INT)                  AS qb_scramble
        , pass_length
        , pass_location
        , CAST(air_yards AS INT)                    AS air_yards
        , CAST(yards_after_catch AS INT)            AS yards_after_catch
        , CAST(qb_hit AS INT)                       AS qb_hit
        , CAST(fumble AS INT)                       AS fumble
        -- Play result information
        , "desc"                                    AS description
        , CAST(yards_gained AS INT)                 AS yards_gained
        , CAST(incomplete_pass AS INT)              AS incomplete_pass
        , CAST(interception AS INT)                 AS interception
        , CAST(pass_attempt AS INT)                 AS pass_attempt
        , CAST(complete_pass AS INT)                AS complete_pass
        , CAST(sack AS INT)                         AS sack
        , CAST(touchdown AS INT)                    AS touchdown
        , CAST(pass_touchdown AS INT)               AS pass_touchdown
        , CAST(passing_yards AS INT)                AS passing_yards
        , CAST(receiving_yards AS INT)              AS receiving_yards
        , interception_player_id        
    FROM {{ ref('int_play_by_play_consolidated') }}
    WHERE play_type = 'pass'
)

SELECT *
FROM src_pass_plays
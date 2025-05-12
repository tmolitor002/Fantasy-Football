WITH src AS (
    SELECT *
    FROM {{ ref('int_play_by_play_consolidated') }}
)

SELECT
    _play_id
    , play_type -- trying to predict
    , "desc" AS description -- helpful to understand whats happening
    , season_type
    , posteam
    , defteam
    , posteam_timeouts_remaining
    , defteam_timeouts_remaining
    , posteam_score
    , defteam_score
    , score_differential
    , CASE
        WHEN posteam = home_team THEN 1
        ELSE 0
        END AS home_pos
    , yardline_100
    , game_seconds_remaining
    , down
    , ydstogo
FROM src
WHERE play_type IS NOT NULL
    AND play_type != 'no_play'
ORDER BY season, week, game_date, play_id ASC

WITH src AS (
    SELECT
        CAST(season AS INT)     AS season
        , CAST(week AS INT)     AS week
        , game_date
        , game_id
        , CAST(play_id AS INT)  AS play_id
        , {{ dbt_utils.star(from=source('football', 'play_by_play_full'), except=[
            'play_id'
            , 'game_id'
            , 'week'
            , 'game_date'
            , 'season'
            , 'defteam_timouts_remaining'
            , 'drive_end_transition'
            , 'home_timeouts_remaining'
            , 'posteam_timeouts_remaining'
            , 'replay_or_challenge_result'
            , 'play_type_nfl'
        ] )}}
        , CASE
            WHEN defteam_timouts_remaining NOT IN ('0', '1', '2', '3') THEN NULL
            ELSE defteam_timouts_remaining
            END AS defteam_timouts_remaining
        , CASE
            WHEN drive_end_transition IN ('Blocked FG', 'BLOCKED_FG') THEN 'BLOCKED_FG'
            WHEN drive_end_transition IN ('Blocked FG, Downs', 'BLOCKED_FG,_DOWNS', 'BLOCKED_FG_DOWNS') THEN 'BLOCKED_FG_DOWNS'
            WHEN drive_end_transition IN ('Blocked Punt', 'BLOCKED_PUNT') THEN 'BLOCKED_PUNT'
            WHEN drive_end_transition IN ('Blocked Punt, Downs', 'BLOCKED_PUNT,_DOWNS', 'BLOCKED_PUNT_DOWNS') THEN 'BLOCKED_PUNT_DOWNS'
            WHEN drive_end_transition IN ('BLOCKED_PUNT,_SAFETY', 'BLOCKED_PUNT_SAFETY') THEN 'BLOCKED_PUNT_SAFETY'
            WHEN drive_end_transition IN ('Downs', 'DOWNS') THEN 'DOWNS'
            WHEN drive_end_transition IN ('END_GAME', 'End of Game') THEN 'END_GAME'
            WHEN drive_end_transition IN ('END_HALF', 'End of Half') THEN 'END_HALF'
            WHEN drive_end_transition IN ('Field Goal', 'FIELD_GOAL') THEN 'FIELD_GOAL'
            WHEN drive_end_transition IN ('Fumble', 'FUMBLE') THEN 'FUMBLE'
            WHEN drive_end_transition IN ('Fumble, Safety', 'FUMBLE,_SAFETY', 'FUMBLE_SAFETY') THEN 'FUMBLE_SAFETY'
            WHEN drive_end_transition IN ('Interception', 'INTERCEPTION') THEN 'INTERCEPTION'
            WHEN drive_end_transition IN ('Missed FG', 'MISSED_FG') THEN 'MISSED_FG'
            WHEN drive_end_transition IN ('Punt', 'PUNT') THEN 'PUNT'
            WHEN drive_end_transition IN ('Safety', 'SAFETY') THEN 'SAFETY'
            WHEN drive_end_transition IN ('Touchdown', 'TOUCHDOWN') THEN 'TOUCHDOWN'
            ELSE 'UNKNOWN'
            END AS drive_end_transition
        , CASE
            WHEN home_timeouts_remaining NOT IN ('0', '1', '2', '3') THEN NULL
            ELSE home_timeouts_remaining
            END AS home_timeouts_remaining
        , CASE
            WHEN posteam_timeouts_remaining NOT IN ('0', '1', '2', '3') THEN NULL
            ELSE posteam_timeouts_remaining
            END AS posteam_timeouts_remaining
        , CASE
            WHEN replay_or_challenge_result IN ('denied', 'upheld') THEN 'upheld'
            ELSE replay_or_challenge_result
            END AS replay_or_challenge_result
        , CASE
            WHEN play_type_nfl = 'UNSPECIFIED' THEN NULL
            ELSE play_type_nfl
            END AS play_type_nfl
    FROM {{ source('football', 'play_by_play_full') }}
)

SELECT *
FROM src
ORDER BY week, game_date, game_id, play_id ASC

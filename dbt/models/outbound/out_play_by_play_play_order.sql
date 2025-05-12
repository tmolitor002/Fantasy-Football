WITH plays AS (
    SELECT
        _play_id
        , game_id
        , season
        , week
        , game_date
        , play_id AS play_sequence
    FROM {{ ref('int_play_by_play_consolidated') }}
)

, games AS (
    SELECT 
        game_id
        , start_time
    FROM {{ ref('int_games_consolidated') }}
)

, play_order AS (
    SELECT
        a._play_id
        , a.season
        , a.week
        , a.game_date
        , a.play_sequence
        , b.start_time
        , a.game_id
    FROM plays a
    LEFT JOIN games b
        ON a.game_id = b.game_id
)

, global_play_order AS (
    SELECT
        _play_id
        , ROW_NUMBER() OVER (ORDER BY start_time, game_id, play_sequence ASC) AS global_play_order
    FROM play_order
)

SELECT *
FROM global_play_order
WITH src AS (
    SELECT
        CAST(season AS INT)     AS season
        , CAST(week AS INT)     AS week
        , game_date
        , game_id
        , CAST(play_id AS INT)  AS play_id
        , {{ dbt_utils.star(from=source('football', 'play_by_play_2024'), except=['play_id', 'game_id', 'week', 'game_date', 'season'] )}}
    FROM {{ source('football', 'play_by_play_2024') }}
)

SELECT *
FROM src
ORDER BY week, game_date, game_id, play_id ASC

WITH src AS (
    SELECT DISTINCT
        game_id
        , season
        , week
        , game_date
        , home_team
        , away_team
        , season_type
        , start_time
        , div_game
        , home_coach
        , away_coach
        -- location info
        , stadium_id
        , game_stadium
        , stadium
        , weather
        , "temp"            AS temperature
        , wind
        , roof
        , surface
        -- game results
        , away_score
        , home_score
            --, location
        , result
        , total
        -- win probabilities and betting
        , spread_line
        , total_line
        -- other
        , nfl_api_id
            -- , "desc"            AS description
            -- , play_type_nfl
        -- metadata
        , _etl_loaded_at -- consider removing if planning to snapshot data
    FROM {{ ref('int_play_by_play_consolidated') }}
    --WHERE "desc" = 'GAME'
)

, cast_types AS (
    SELECT
        game_id
        , season
        , week
        , game_date
        , home_team
        , away_team
        , season_type
        , start_time
        , CAST(div_game AS INT)         AS div_game
        , home_coach
        , away_coach
        , stadium_id
        , stadium
        , weather
        , temperature
        , wind
        , roof
        , surface
        , CAST(away_score AS INT)       AS away_score
        , CAST(home_score AS INT)       AS home_score
        , CAST(result AS INT)           AS result
        , CAST(total AS INT)            AS total
        , CAST(spread_line AS DECIMAL)  AS spread_line
        , CAST(total_line AS DECIMAL)   AS total_line
        , nfl_api_id
        , _etl_loaded_at
    FROM src
)

SELECT *
FROM cast_types
ORDER BY season, week, game_date, game_id ASC

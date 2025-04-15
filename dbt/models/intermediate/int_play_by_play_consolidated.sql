WITH unioned AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_play_by_play_2022')
            , ref('stg_play_by_play_2023')
            , ref('stg_play_by_play_2024')
        ]
    ) }}
)

, pos_and_def_coach AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['game_id', 'play_id']) }} AS _play_id
        , *
        , CASE
            WHEN posteam_type = 'away' THEN away_coach
            WHEN posteam_type = 'home' THEN home_coach
            ELSE NULL
            END AS pos_coach
        , CASE
            WHEN posteam_type = 'away' THEN home_coach
            WHEN posteam_type = 'home' THEN away_coach
            ELSE NULL
            END AS def_coach
    FROM unioned
)

, final AS (
    SELECT
        *        
    FROM pos_and_def_coach
)

SELECT *
FROM final
ORDER BY season, week, game_date, game_id, play_id ASC

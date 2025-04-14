WITH unioned AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_play_by_play_2022')
            , ref('stg_play_by_play_2023')
            , ref('stg_play_by_play_2024')
        ]
    ) }}
)

, final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['game_id', 'play_id']) }} AS _play_id
        , *
    FROM unioned
)

SELECT *
FROM final
ORDER BY season, week, game_date, game_id, play_id ASC

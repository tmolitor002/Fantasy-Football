WITH src AS (
    SELECT
        {{ dbt_utils.star(from=source('dataiku','FANTASYFOOTBALL_iku_play_by_play_rushing_scored_prepared'), except=[
            'smmd_predictionTime'
            , 'smmd_savedModelID'
            , 'smmd_modelVersion'
            , 'smmd_fullModelId'
            ])}}
        -- model metadata
        , "smmd_savedModelId"                                               AS smmd_saved_model_id
        , "smmd_modelVersion"                                               AS smmd_model_version
        , "smmd_fullModelId"                                                AS smmd_full_model_id
        , TIMEZONE('America/Chicago',"smmd_predictionTime"::TIMESTAMPTZ)    AS ssmd_prediction_time_chicago
    FROM {{ source('dataiku', 'FANTASYFOOTBALL_iku_play_by_play_rushing_scored_prepared') }}
)

, final AS (
    SELECT *
    FROM src
)

SELECT *
FROM final
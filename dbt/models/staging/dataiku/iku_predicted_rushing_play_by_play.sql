WITH src AS (
    SELECT *
    FROM {{ source('dataiku', 'FANTASYFOOTBALL_iku_play_by_play_rushing_scored_prepared') }}
)

SELECT *
FROM src
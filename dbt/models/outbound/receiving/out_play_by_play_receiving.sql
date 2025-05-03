-- TODO: Select only pre-snap fields along with fantasy_points and _play_id for ML model

WITH src AS (
    SELECT *
    FROM {{ ref('int_play_by_play_receiving_fantasy_scoring') }}
)

SELECT *
FROM src
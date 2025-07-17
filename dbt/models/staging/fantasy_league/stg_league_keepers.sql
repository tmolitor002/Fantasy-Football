WITH src AS (
    SELECT *
    FROM {{ source('football', 'ff_keeper_picks') }}
)

SELECT *
FROM src
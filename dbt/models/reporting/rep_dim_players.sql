WITH final AS (
    SELECT
        gsis_id AS player_id
        , CASE -- Add suffix to display names
            WHEN suffix IS NOT NULL THEN CONCAT(display_name, CONCAT(' ', suffix))
            ELSE display_name
            END AS display_name_suffix
        , {{ dbt_utils.star(from=ref('stg_players'), except=['gsis_id']) }}
    FROM {{ ref('stg_players') }}
)

SELECT *
FROM final
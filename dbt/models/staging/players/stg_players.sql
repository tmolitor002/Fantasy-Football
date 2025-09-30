WITH src AS (
    SELECT
        *
    FROM {{ source('football', 'players') }}
)

-- clean and reorder columns
, clean AS (
    SELECT
        -- IDs
        gsis_id
        , current_team_id
        -- names
        , {{ stage('display_name') }}
        , {{ stage('first_name') }}
        , {{ stage('last_name') }}
        , {{ stage('football_name') }}
        , {{ stage('short_name') }}
        , {{ stage('suffix') }}
        -- other player info
        , CASE
            WHEN jersey_number = '' THEN CAST(NULL AS INT)
            ELSE CAST(jersey_number AS INT)
            END AS jersey_number
        , {{ stage('team_abbr') }}
        , {{ stage('headshot') }}
        , {{ stage('position') }}
        , {{ stage('position_group')}}
        , CASE
            WHEN rookie_year = '' THEN CAST(NULL AS INT)
            ELSE CAST(rookie_year AS INT)
            END AS rookie_year
        , {{ stage('uniform_number') }}
        -- player measurements
        , CASE
            WHEN height = '' THEN CAST(NULL AS INT)
            ELSE CAST(height AS INT)
            END AS height_inches
        , CASE
            WHEN weight = '' THEN CAST(NULL AS INT)
            ELSE CAST(weight AS INT)
            END AS weight_pounds
        , {{ stage('birth_date') }}
        , CASE
            WHEN team_seq = '' THEN CAST(NULL AS INT)
            ELSE CAST(team_seq AS INT)
            END AS team_seq
        -- status fields
        , {{ stage('status') }}
        , {{ stage('status_description_abbr')}}
        , {{ stage('status_short_description')}}
        -- draft info
        , {{ stage('draft_club') }}
        , CASE
            WHEN draft_number = '' THEN CAST(NULL AS INT)
            ELSE CAST(draft_number AS INT)
            END AS draft_number
        , CASE
            WHEN draftround = '' THEN CAST(NULL AS INT)
            ELSE CAST(draftround AS INT)
            END AS draft_round
        , CASE
            WHEN entry_year = '' THEN CAST(NULL AS INT)
            ELSE CAST(entry_year AS INT)
            END AS entry_year
        , CASE
            WHEN years_of_experience = '' THEN CAST(NULL AS INT)
            ELSE CAST(years_of_experience AS INT)
            END AS years_of_experience
        -- college info
        , {{ stage('college_name' )}}
        , {{ stage('college_conference') }}
        -- metadata
        , esb_id
        , smart_id
        , gsis_it_id
        , _etl_loaded_at
    FROM src
)

, final AS (
    SELECT *
    FROM clean
    WHERE
        gsis_id != ''
        AND gsis_id IS NOT NULL
)

SELECT *
FROM final

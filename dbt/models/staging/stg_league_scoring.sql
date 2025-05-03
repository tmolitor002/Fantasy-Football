WITH league_scoring AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['platform', 'league']) }} AS league_id
        , *
    FROM {{ ref('seed_league_scoring') }}
    WHERE league = '{{ var('active_league') }}' -- Only want to calculate fantasy points for one league at a time. Allows multiple leagues to be saved in seed, and adjustment made to project variable
)

SELECT *
FROM league_scoring

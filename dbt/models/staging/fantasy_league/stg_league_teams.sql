WITH teams AS (
    SELECT
        team           AS team_name
        , manager      AS manager_name
        , league_id    AS league_id
    FROM {{ source('football', 'ff_league_teams') }}
)

, leagues AS (
    SELECT
        league_id
        , platform
        , league AS league_name
    FROM {{ ref('seed_league_scoring') }}
)

, league_teams AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['b.platform', 'b.league_name']) }} AS _league_id
        , {{ dbt_utils.generate_surrogate_key(['a.manager_name']) }} AS _team_id -- rethink how to assign these at scale...especially if incorporating any yahoo apis
        , {{ dbt_utils.generate_surrogate_key(['a.manager_name']) }} AS _manager_id
        , b.platform
        , b.league_name
        , a.team_name
        , a.manager_name
    FROM teams a
    JOIN leagues b
        ON a.league_id = b.league_id
)

SELECT *
FROM league_teams

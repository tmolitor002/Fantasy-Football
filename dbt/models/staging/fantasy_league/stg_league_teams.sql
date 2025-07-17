WITH teams AS (
    SELECT
        team                AS team_name
        , manager           AS manager_name
        , league_id::INT    AS league_id_int
    FROM {{ source('football', 'ff_league_teams') }}
)

, leagues AS (
    SELECT
        league_id::INT   AS league_id_int
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
        ON a.league_id_int = b.league_id_int
)

SELECT *
FROM league_teams

-- ToDo: Add kickers and defenses

WITH receiving AS (
    {{ dbt_utils.unpivot(
        relation=ref('int_game_consolidated_receiving'),
        cast_to='DECIMAL',
        exclude=['game_id', 'receiver_player_id'],
        remove=['posteam', 'pos_coach', 'defteam', 'def_coach', 'display_name', 'position'],
        field_name='stat',
        value_name='value',
    ) }}
)

, receiving_rename AS (
    SELECT
        game_id
        , receiver_player_id            AS player_id
        , CONCAT('receiving_', stat)    AS stat
        , value
    FROM receiving
)

, rushing AS (
    {{ dbt_utils.unpivot(
        relation=ref('int_game_consolidated_rushing'),
        cast_to='DECIMAL',
        exclude=['game_id', 'rusher_player_id'],
        remove=['posteam', 'pos_coach', 'defteam', 'def_coach', 'display_name', 'position'],
        field_name='stat',
        value_name='value'

    ) }}
)

, rushing_rename AS (
    SELECT
        game_id
        , rusher_player_id          AS player_id
        , CONCAT('rushing_', stat)  AS stat
        , value
    FROM rushing
)

, passing AS (
    {{ dbt_utils.unpivot(
        relation=ref('int_game_consolidated_passing'),
        cast_to='DECIMAL',
        exclude=['game_id', 'passer_player_id'],
        remove=['posteam', 'pos_coach', 'defteam', 'def_coach', 'display_name', 'position'],
        field_name='stat',
        value_name='value'
    ) }}
)

, passing_rename AS (
    SELECT
        game_id
        , passer_player_id          AS player_id
        , CONCAT('passing_', stat)  AS stat
        , value
    FROM passing
)

, union_all AS (
    SELECT *
    FROM receiving_rename
    UNION
    SELECT *
    FROM rushing_rename
    UNION
    SELECT *
    FROM passing_rename
)

SELECT *
FROM union_all
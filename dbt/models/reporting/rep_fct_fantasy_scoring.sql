WITH scoring AS (
    {{ dbt_utils.unpivot(
        relation=ref('int_scoring_renamed'),
        cast_to='DECIMAL',
        exclude=['league_id', 'league'],
        remove=['platform', 'fractional_points'],
        field_name='stat',
        value_name='value'
    ) }}
)

, final AS (
    SELECT
        league_id
        , league
        , SPLIT_PART(stat, '_', 1) AS stat_category
        , RIGHT(
            stat
            , LENGTH(
                stat
            ) - LENGTH(
                SPLIT_PART(stat, '_', 1)
            ) - 1
        ) AS stat
        , stat AS full_stat
        , value
    FROM scoring
)

SELECT *
FROM final
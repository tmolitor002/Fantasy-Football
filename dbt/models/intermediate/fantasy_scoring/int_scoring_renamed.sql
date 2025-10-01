-- TODO:
    -- calculate additional passing metrics

WITH scoring AS (
    -- Rename scoring multipliers to align with rep_fct_player_game_stats
    SELECT
        {{ dbt_utils.star(from=ref('stg_league_scoring'), except=[            
            'passing_touchdown'
            , 'passing_interception'
            , 'passing_total_pass_attempts'
            , 'passing_completion'
            , 'passing_incomplete'
            , 'passing_sack'
            , 'passing_yards_pp'
            , 'passing_pick_six'
            , 'passing_40_yd_plus_completion'
            , 'passing_40_yd_plus_touchdown'
            , 'passing_first_down'
            , 'receiving_touchdown'
            , 'receiving_reception'
            , 'receiving_yards_pp'
            , 'receiving_40_yd_plus'
            , 'rushing_touchdown'
            , 'rushing_attempt'
            , 'rushing_yards_pp'
            , 'rushing_40_yd_plus_rush'
            , 'rushing_40_yd_plus_touchdown'
            , 'rushing_first_down'
            , 'kick_punt_return_touchdown'
            , 'kick_punt_return_yards_pp'
            , 'two_point_conversion'
            , 'fumble'
            , 'fg_made_0_19'
            , 'fg_made_20_29'
            , 'fg_made_30_39'
            , 'fg_made_40_49'
            , 'fg_made_50_plus'
            , 'fg_miss_0_19'
            , 'fg_miss_20_29'
            , 'fg_miss_30_39'
            , 'fg_miss_40_49'
            , 'fg_miss_50_plus'
            , 'fg_yards_pp'
            , 'pat_made'
            , 'pat_miss'
            , 'def_points_allowed_0'
            , 'def_points_allowed_1_6'
            , 'def_points_allowed_7_13'
            , 'def_points_allowed_14_20'
            , 'def_points_allowed_21_27'
            , 'def_points_allowed_28_34'
            , 'def_points_allowed_35_plus'
            , 'def_sack'
            , 'def_interception'
            , 'def_fumble_recovery'
            , 'def_touchdown'
            , 'def_safety'
            , 'def_block_kick'
            , 'def_return_yards_pp'
            , 'def_kickoff_punt_return_touchdown'
            , 'def_fourth_down_stop'
            , 'def_tackle_for_loss'
            , 'def_yards_allowed_negative'
            , 'def_yards_allowed_0_99'
            , 'def_yards_allowed_100_199'
            , 'def_yards_allowed_200_299'
            , 'def_yards_allowed_300_399'
            , 'def_yards_allowed_400_499'
            , 'def_yards_allowed_500_plus'
            , 'def_three_and_out_forced'
            , 'def_extra_point_returned'
            ]) 
        }}
        -- Passing
        , passing_touchdown         AS passing_total_passing_touchdowns
        , passing_interception      AS passing_total_interceptions
        , passing_attempts          AS passing_total_pass_attempts
        , passing_completion        AS passing_total_complete_pass_attempts
        , passing_incomplete        AS passing_total_incomplete_pass_attempts
        , passing_sack              AS passing_total_sacks
        , 1 / passing_yards_pp      AS passing_total_yards_gained
        , fumble_lost               AS passing_total_fumbles -- assuming a fumble will be lost, will be innacurate
        -- Need to calculate in int_game_consolidated_passing
            -- , passing_pick_six
            -- , passing_40_yd_plus_completion
            -- , passing_40_yd_plus_touchdown
            -- , passing_first_down
        -- Receiving
        , receiving_touchdown       AS receiving_total_receiving_touchdowns
        , receiving_reception       AS receiving_total_receptions
        , 1 / receiving_yards_pp    AS receiving_total_receiving_yards
        , fumble_lost               AS receiving_total_fumbles -- assuming a fumble will be lost, will be innacurate
        -- Need to calculate in int_game_consolidated_receiving
            -- , receiving_40_yd_plus_reception
            -- , receiving_40_yd_plus_touchdown
            -- , receivintg_first_down
        -- Rushing
        , rushing_touchdown         AS rushing_total_touchdowns
        , rushing_attempt           AS rushing_total_rush_attempts
        , 1 / rushing_yards_pp      AS rushing_total_rushing_yards
        , fumble_lost               AS rushing_total_fumbles -- assuming a fumble will be lost, will be innacurate
        -- Need to calculate in int_game_consolidated_rushing
            -- , rushing_40_yd_plus_rush
            -- , rushing_40_yd_plus_touchdown
            -- , rushing_first_down
        /* Kicking/Defense
        , kick_punt_return_touchdown
        , kick_punt_return_yard
        , two_point_conversion
        , fumble
        , fg_made_0_19
        , fg_made_20_29
        , fg_made_30_39
        , fg_made_40_49
        , fg_made_50_plus
        , fg_miss_0_19
        , fg_miss_20_29
        , fg_miss_30_39
        , fg_miss_40_49
        , fg_miss_50_plus
        , fg_yards_pp
        , pat_made
        , pat_miss
        , def_points_allowed_0
        , def_points_allowed_1_6
        , def_points_allowed_7_13
        , def_points_allowed_14_20
        , def_points_allowed_21_27
        , def_points_allowed_28_34
        , def_points_allowed_35_plus
        , def_sack
        , def_interception
        , def_fumble_recovery
        , def_touchdown
        , def_safety
        , def_block_kick
        , def_return_yards_pp
        , def_kickoff_punt_return_touchdown
        , def_fourth_down_stop
        , def_tackle_for_loss
        , def_yards_allowed_negative
        , def_yards_allowed_0_99
        , def_yards_allowed_100_199
        , def_yards_allowed_200_299
        , def_yards_allowed_300_399
        , def_yards_allowed_400_499
        , def_yards_allowed_500_plus
        , def_three_and_out_forced
        , def_extra_point_returned
        */
    FROM {{ ref('stg_league_scoring') }}
)

SELECT *
FROM scoring
WITH unioned AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_play_by_play_2022')
            , ref('stg_play_by_play_2023')
            , ref('stg_play_by_play_2024')
        ]
    ) }}
)

, cast_types AS (
    SELECT
        _dbt_source_relation
        , CAST(season                           AS INT) AS season
        , CAST(week                             AS INT) AS week
        , game_date
        , game_id
        , CAST(play_id                          AS INT) AS play_id
        , old_game_id
        , home_team
        , away_team
        , season_type
        , posteam
        , posteam_type
        , defteam
        , side_of_field
        , CAST(yardline_100                     AS INT) AS yardline_100
        , CAST(quarter_seconds_remaining        AS INT) AS quarter_seconds_remaining
        , CAST(half_seconds_remaining           AS INT) AS half_seconds_remaining
        , CAST(game_seconds_remaining           AS INT) AS game_seconds_remaining
        , game_half
        , CAST(quarter_end                      AS INT) AS quarter_end
        , CAST(drive                            AS INT) AS drive
        , CAST(sp                               AS INT)	AS sp
        , CAST(quarter_end                      AS INT) AS qtr
        , CAST(down                             AS INT) AS down
        , CAST(goal_to_go                       AS INT) AS goal_to_go
        , "time"
        , yrdln
        , CAST(ydstogo                          AS INT) AS ydstogo
        , CAST(ydsnet                           AS INT) AS ydsnet
        , "desc"
        , play_type
        , CAST(yards_gained                     AS INT) AS yards_gained
        , CAST(shotgun                          AS INT) AS shotgun
        , CAST(no_huddle                        AS INT) AS no_huddle
        , CAST(qb_dropback                      AS INT) AS qb_dropback
        , CAST(qb_kneel                         AS INT) AS qb_kneel
        , CAST(qb_spike                         AS INT) AS qb_spike
        , CAST(qb_scramble                      AS INT) AS qb_scramble
        , pass_length
        , pass_location
        , CAST(air_yards                        AS INT) AS air_yards
        , CAST(yards_after_catch                AS INT) AS yards_after_catch
        , run_location
        , run_gap
        , field_goal_result
        , CAST(kick_distance                    AS INT) AS kick_distance
        , extra_point_result
        , two_point_conv_result
        , CAST(home_timeouts_remaining          AS INT) AS home_timeouts_remaining
        , CAST(away_timeouts_remaining          AS INT) AS away_timeouts_remaining
        , CAST(timeout                          AS INT) AS timeout
        , timeout_team
        , td_team
        , td_player_name
        , td_player_id
        , CAST(posteam_timeouts_remaining       AS INT) AS posteam_timeouts_remaining
        , CAST(defteam_timeouts_remaining       AS INT) AS defteam_timeouts_remaining
        , CAST(total_home_score                 AS INT) AS total_home_score
        , CAST(total_away_score                 AS INT) AS total_away_score
        , CAST(posteam_score                    AS INT) AS posteam_score
        , CAST(defteam_score                    AS INT) AS defteam_score
        , CAST(score_differential               AS INT) AS score_differential
        , CAST(posteam_score_post               AS INT) AS posteam_score_post
        , CAST(defteam_score_post               AS INT) AS defteam_score_post
        , CAST(score_differential_post          AS INT) AS score_differential_post
        , CAST(no_score_prob                    AS DECIMAL) AS no_score_prob
        , CAST(opp_fg_prob                      AS DECIMAL) AS opp_fg_prob
        , CAST(opp_safety_prob                  AS DECIMAL) AS opp_safety_prob
        , CAST(opp_td_prob                      AS DECIMAL) AS opp_td_prob
        , CAST(fg_prob                          AS DECIMAL) AS fg_prob
        , CAST(safety_prob                      AS DECIMAL) AS safety_prob
        , CAST(td_prob                          AS DECIMAL) AS td_prob
        , CAST(extra_point_prob                 AS DECIMAL) AS extra_point_prob
        , CAST(two_point_conversion_prob        AS DECIMAL) AS two_point_conversion_prob
        , CAST(ep                               AS DECIMAL) AS ep
        , CAST(epa                              AS DECIMAL) AS epa
        , CAST(total_home_epa                   AS DECIMAL) AS total_home_epa
        , CAST(total_away_epa                   AS DECIMAL) AS total_away_epa
        , CAST(total_home_rush_epa              AS DECIMAL) AS total_home_rush_epa
        , CAST(total_away_rush_epa              AS DECIMAL) AS total_away_rush_epa
        , CAST(total_home_pass_epa              AS DECIMAL) AS total_home_pass_epa
        , CAST(total_away_pass_epa              AS DECIMAL) AS total_away_pass_epa
        , CAST(air_epa                          AS DECIMAL) AS air_epa
        , CAST(yac_epa                          AS DECIMAL) AS yac_epa
        , CAST(comp_air_epa                     AS DECIMAL) AS comp_air_epa
        , CAST(comp_yac_epa                     AS DECIMAL) AS comp_yac_epa
        , CAST(total_home_comp_air_epa          AS DECIMAL) AS total_home_comp_air_epa
        , CAST(total_away_comp_air_epa          AS DECIMAL) AS total_away_comp_air_epa
        , CAST(total_home_comp_yac_epa          AS DECIMAL) AS total_home_comp_yac_epa
        , CAST(total_away_comp_yac_epa          AS DECIMAL) AS total_away_comp_yac_epa
        , CAST(total_home_raw_air_epa           AS DECIMAL) AS total_home_raw_air_epa
        , CAST(total_away_raw_air_epa           AS DECIMAL) AS total_away_raw_air_epa
        , CAST(total_home_raw_yac_epa           AS DECIMAL) AS total_home_raw_yac_epa
        , CAST(total_away_raw_yac_epa           AS DECIMAL) AS total_away_raw_yac_epa
        , CAST(wp                               AS DECIMAL) AS wp
        , CAST(def_wp                           AS DECIMAL) AS def_wp
        , CAST(home_wp                          AS DECIMAL) AS home_wp
        , CAST(away_wp                          AS DECIMAL) AS away_wp
        , CAST(wpa                              AS DECIMAL) AS wpa
        , CAST(vegas_wpa                        AS DECIMAL) AS vegas_wpa
        , CAST(vegas_home_wpa                   AS DECIMAL) AS vegas_home_wpa
        , CAST(home_wp_post                     AS DECIMAL) AS home_wp_post
        , CAST(away_wp_post                     AS DECIMAL) AS away_wp_post
        , CAST(vegas_wp                         AS DECIMAL) AS vegas_wp
        , CAST(vegas_home_wp                    AS DECIMAL) AS vegas_home_wp
        , CAST(total_home_rush_wpa              AS DECIMAL) AS total_home_rush_wpa
        , CAST(total_away_rush_wpa              AS DECIMAL) AS total_away_rush_wpa
        , CAST(total_home_pass_wpa              AS DECIMAL) AS total_home_pass_wpa
        , CAST(total_away_pass_wpa              AS DECIMAL) AS total_away_pass_wpa
        , CAST(air_wpa                          AS DECIMAL) AS air_wpa
        , CAST(yac_wpa                          AS DECIMAL) AS yac_wpa
        , CAST(comp_air_wpa                     AS DECIMAL) AS comp_air_wpa
        , CAST(comp_yac_wpa                     AS DECIMAL) AS comp_yac_wpa
        , CAST(total_home_comp_air_wpa          AS DECIMAL) AS total_home_comp_air_wpa
        , CAST(total_away_comp_air_wpa          AS DECIMAL) AS total_away_comp_air_wpa
        , CAST(total_home_comp_yac_wpa          AS DECIMAL) AS total_home_comp_yac_wpa
        , CAST(total_away_comp_yac_wpa          AS DECIMAL) AS total_away_comp_yac_wpa
        , CAST(total_home_raw_air_wpa           AS DECIMAL) AS total_home_raw_air_wpa
        , CAST(total_away_raw_air_wpa           AS DECIMAL) AS total_away_raw_air_wpa
        , CAST(total_home_raw_yac_wpa           AS DECIMAL) AS total_home_raw_yac_wpa
        , CAST(total_away_raw_yac_wpa           AS DECIMAL) AS total_away_raw_yac_wpa
        , CAST(punt_blocked                     AS INT) AS punt_blocked
        , CAST(first_down_rush                  AS INT) AS first_down_rush
        , CAST(first_down_pass                  AS INT) AS first_down_pass
        , CAST(first_down_penalty               AS INT) AS first_down_penalty
        , CAST(third_down_converted             AS INT) AS third_down_converted
        , CAST(third_down_failed                AS INT) AS third_down_failed
        , CAST(fourth_down_converted            AS INT) AS fourth_down_converted
        , CAST(fourth_down_failed               AS INT) AS fourth_down_failed
        , CAST(incomplete_pass                  AS INT) AS incomplete_pass
        , CAST(touchback                        AS INT) AS touchback
        , CAST(interception                     AS INT) AS interception
        , CAST(punt_inside_twenty               AS INT) AS punt_inside_twenty
        , CAST(punt_in_endzone                  AS INT) AS punt_in_endzone
        , CAST(punt_out_of_bounds               AS INT) AS punt_out_of_bounds
        , CAST(punt_downed                      AS INT) AS punt_downed
        , CAST(punt_fair_catch                  AS INT) AS punt_fair_catch
        , CAST(kickoff_inside_twenty            AS INT) AS kickoff_inside_twenty
        , CAST(kickoff_in_endzone               AS INT) AS kickoff_in_endzone
        , CAST(kickoff_out_of_bounds            AS INT) AS kickoff_out_of_bounds
        , CAST(kickoff_downed                   AS INT) AS kickoff_downed
        , CAST(kickoff_fair_catch               AS INT) AS kickoff_fair_catch
        , CAST(fumble_forced                    AS INT) AS fumble_forced
        , CAST(fumble_not_forced                AS INT) AS fumble_not_forced
        , CAST(fumble_out_of_bounds             AS INT) AS fumble_out_of_bounds
        , CAST(solo_tackle                      AS INT) AS solo_tackle
        , CAST(safety                           AS INT) AS safety
        , CAST(penalty                          AS INT) AS penalty
        , CAST(tackled_for_loss                 AS INT) AS tackled_for_loss
        , CAST(fumble_lost                      AS INT) AS fumble_lost
        , CAST(own_kickoff_recovery             AS INT) AS own_kickoff_recovery
        , CAST(own_kickoff_recovery_td          AS INT) AS own_kickoff_recovery_td
        , CAST(qb_hit                           AS INT) AS qb_hit
        , CAST(rush_attempt                     AS INT) AS rush_attempt
        , CAST(pass_attempt                     AS INT) AS pass_attempt
        , CAST(sack                             AS INT) AS sack
        , CAST(touchdown                        AS INT) AS touchdown
        , CAST(pass_touchdown                   AS INT) AS pass_touchdown
        , CAST(rush_touchdown                   AS INT) AS rush_touchdown
        , CAST(return_touchdown                 AS INT) AS return_touchdown
        , CAST(extra_point_attempt              AS INT) AS extra_point_attempt
        , CAST(two_point_attempt                AS INT) AS two_point_attempt
        , CAST(field_goal_attempt               AS INT) AS field_goal_attempt
        , CAST(kickoff_attempt                  AS INT) AS kickoff_attempt
        , CAST(punt_attempt                     AS INT) AS punt_attempt
        , CAST(fumble                           AS INT) AS fumble
        , CAST(complete_pass                    AS INT) AS complete_pass
        , CAST(assist_tackle                    AS INT) AS assist_tackle
        , CAST(lateral_reception                AS INT) AS lateral_reception
        , CAST(lateral_rush                     AS INT) AS lateral_rush
        , CAST(lateral_return                   AS INT) AS lateral_return
        , CAST(lateral_recovery                 AS INT) AS lateral_recovery
        , passer_player_id
        , passer_player_name
        , CAST(passing_yards                    AS INT) AS passing_yards
        , receiver_player_id
        , receiver_player_name
        , CAST(receiving_yards                  AS INT) AS receiving_yards
        , rusher_player_id
        , rusher_player_name
        , CAST(rushing_yards                    AS INT) AS rushing_yards
        , lateral_receiver_player_id
        , lateral_receiver_player_name
        , CAST(lateral_receiving_yards          AS INT) AS lateral_receiving_yards
        , lateral_rusher_player_id
        , lateral_rusher_player_name
        , CAST(lateral_rushing_yards            AS INT) AS lateral_rushing_yards
        , lateral_sack_player_id
        , lateral_sack_player_name
        , interception_player_id
        , interception_player_name
        , lateral_interception_player_id
        , lateral_interception_player_name
        , punt_returner_player_id
        , punt_returner_player_name
        , lateral_punt_returner_player_id
        , lateral_punt_returner_player_name
        , kickoff_returner_player_name
        , kickoff_returner_player_id
        , lateral_kickoff_returner_player_id
        , lateral_kickoff_returner_player_name
        , punter_player_id
        , punter_player_name
        , kicker_player_name
        , kicker_player_id
        , own_kickoff_recovery_player_id
        , own_kickoff_recovery_player_name
        , blocked_player_id
        , blocked_player_name
        , tackle_for_loss_1_player_id
        , tackle_for_loss_1_player_name
        , tackle_for_loss_2_player_id
        , tackle_for_loss_2_player_name
        , qb_hit_1_player_id
        , qb_hit_1_player_name
        , qb_hit_2_player_id
        , qb_hit_2_player_name
        , forced_fumble_player_1_team
        , forced_fumble_player_1_player_id
        , forced_fumble_player_1_player_name
        , forced_fumble_player_2_team
        , forced_fumble_player_2_player_id
        , forced_fumble_player_2_player_name
        , solo_tackle_1_team
        , solo_tackle_2_team
        , solo_tackle_1_player_id
        , solo_tackle_2_player_id
        , solo_tackle_1_player_name
        , solo_tackle_2_player_name
        , assist_tackle_1_player_id
        , assist_tackle_1_player_name
        , assist_tackle_1_team
        , assist_tackle_2_player_id
        , assist_tackle_2_player_name
        , assist_tackle_2_team
        , assist_tackle_3_player_id
        , assist_tackle_3_player_name
        , assist_tackle_3_team
        , assist_tackle_4_player_id
        , assist_tackle_4_player_name
        , assist_tackle_4_team
        , tackle_with_assist
        , tackle_with_assist_1_player_id
        , tackle_with_assist_1_player_name
        , tackle_with_assist_1_team
        , tackle_with_assist_2_player_id
        , tackle_with_assist_2_player_name
        , tackle_with_assist_2_team
        , pass_defense_1_player_id
        , pass_defense_1_player_name
        , pass_defense_2_player_id
        , pass_defense_2_player_name
        , fumbled_1_team
        , fumbled_1_player_id
        , fumbled_1_player_name
        , fumbled_2_player_id
        , fumbled_2_player_name
        , fumbled_2_team
        , fumble_recovery_1_team
        , CAST(fumble_recovery_1_yards          AS INT) AS fumble_recovery_1_yards
        , fumble_recovery_1_player_id
        , fumble_recovery_1_player_name
        , fumble_recovery_2_team
        , CAST(fumble_recovery_2_yards          AS INT) AS fumble_recovery_2_yards
        , fumble_recovery_2_player_id
        , fumble_recovery_2_player_name
        , sack_player_id
        , sack_player_name
        , half_sack_1_player_id
        , half_sack_1_player_name
        , half_sack_2_player_id
        , half_sack_2_player_name
        , return_team
        , CAST(return_yards                     AS INT) AS return_yards
        , penalty_team
        , penalty_player_id
        , penalty_player_name
        , CAST(penalty_yards                    AS INT) AS penalty_yards
        , replay_or_challenge
        , replay_or_challenge_result
        , penalty_type
        , CAST(defensive_two_point_attempt      AS INT) AS defensive_two_point_attempt
        , CAST(defensive_two_point_conv         AS INT) AS defensive_two_point_conv
        , CAST(defensive_extra_point_attempt    AS INT) AS defensive_extra_point_attempt
        , CAST(defensive_extra_point_conv       AS INT) AS defensive_extra_point_conv
        , safety_player_name
        , safety_player_id
        , CAST(cp                               AS DECIMAL) AS cp
        , CAST(cpoe                             AS DECIMAL) AS cpoe
        , CAST(series                           AS INT) AS series
        , CAST(series_success                   AS INT) AS series_success
        , series_result
        , CAST(order_sequence                   AS INT) AS order_sequence
        , start_time
        , time_of_day
        , stadium
        , weather
        , nfl_api_id
        , play_clock
        , CAST(play_deleted                     AS INT) AS play_deleted
        , play_type_nfl
        , CAST(special_teams_play               AS INT) AS special_teams_play
        , st_play_type
        , end_clock_time
        , end_yard_line
        , CAST(fixed_drive                      AS INT) AS fixed_drive
        , fixed_drive_result
        , drive_real_start_time
        , CAST(drive_play_count                 AS INT) AS drive_play_count
        , drive_time_of_possession
        , CAST(drive_first_downs                AS INT) AS drive_first_downs
        , CAST(drive_inside20                   AS INT) AS drive_inside20
        , CAST(drive_ended_with_score           AS INT) AS drive_ended_with_score
        , CAST(drive_quarter_start              AS INT) AS drive_quarter_start
        , CAST(drive_quarter_end                AS INT) AS drive_quarter_end
        , CAST(drive_yards_penalized            AS INT) AS drive_yards_penalized
        , drive_start_transition
        , drive_end_transition
        , drive_game_clock_start
        , drive_game_clock_end
        , drive_start_yard_line
        , drive_end_yard_line
        , CAST(drive_play_id_started            AS INT) AS drive_play_id_started
        , CAST(drive_play_id_ended              AS INT) AS drive_play_id_ended
        , CAST(away_score                       AS INT) AS away_score
        , CAST(home_score                       AS INT) AS home_score
        , location
        , CAST(result                           AS INT) AS result
        , CAST(total                            AS INT) AS total
        , CAST(spread_line                      AS DECIMAL) AS spread_line
        , CAST(total_line                       AS DECIMAL) AS total_line
        , CAST(div_game                         AS INT) AS div_game
        , roof
        , surface
        , "temp"
        , wind
        , home_coach
        , away_coach
        , stadium_id
        , game_stadium
        , CAST(aborted_play                     AS INT) AS aborted_play
        , CAST(success                          AS INT) AS success
        , passer
        , passer_jersey_number
        , rusher
        , rusher_jersey_number
        , receiver
        , receiver_jersey_number
        , CAST(pass                             AS INT) AS pass
        , CAST(rush                             AS INT) AS rush
        , CAST(first_down                       AS INT) AS first_down
        , CAST(special                          AS INT) AS special
        , CAST(play                             AS INT) AS play
        , passer_id
        , rusher_id
        , receiver_id
        , name
        , jersey_number
        , id
        , fantasy_player_name
        , fantasy_player_id
        , fantasy
        , fantasy_id
        , CAST(out_of_bounds                    AS INT) AS out_of_bounds
        , CAST(home_opening_kickoff             AS INT) AS home_opening_kickoff
        , CAST(qb_epa                           AS DECIMAL) AS qb_epa
        , CAST(xyac_epa                         AS DECIMAL) AS xyac_epa
        , CAST(xyac_mean_yardage                AS DECIMAL) AS xyac_mean_yardage
        , CAST(xyac_median_yardage              AS INT) AS xyac_median_yardage
        , CAST(xyac_success                     AS DECIMAL) AS xyac_success
        , CAST(xyac_fd                          AS DECIMAL) AS xyac_fd
        , CAST(xpass                            AS DECIMAL) AS xpass
        , CAST(pass_oe                          AS DECIMAL) AS pass_oe
        , _etl_loaded_at
    FROM unioned
)


-- add additional calcualted fields
, pos_and_def_coach AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['game_id', 'play_id']) }} AS _play_id
        , *
        , CASE
            WHEN posteam_type = 'away' THEN away_coach
            WHEN posteam_type = 'home' THEN home_coach
            ELSE NULL
            END AS pos_coach
        , CASE
            WHEN posteam_type = 'away' THEN home_coach
            WHEN posteam_type = 'home' THEN away_coach
            ELSE NULL
            END AS def_coach
    FROM cast_types
)

, final AS (
    SELECT
        *        
    FROM pos_and_def_coach
)

SELECT *
FROM final
ORDER BY season, week, game_date, game_id, play_id ASC

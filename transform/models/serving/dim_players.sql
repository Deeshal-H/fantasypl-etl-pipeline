with stg_players as (
    select
        player_id,
        team_id,
        web_name,
        first_name,
        second_name,
        total_points,
        points_per_game,
        total_bonus,
        now_cost,
        status,
        total_goals_scored,
        total_assists,
        total_goals_conceded,
        total_own_goals,
        total_minutes,
        position,
        total_penalties,
        total_starts,
        starts_per_90,
        saves_per_90,
        goals_conceded_per_90,
        total_expected_goals,
        total_expected_assists,
        total_expected_goal_involvements,
        total_expected_goals_conceded,
        expected_goal_involvements_per_90,
        expected_goals_per_90,
        expected_assists_per_90,
        expected_goals_conceded_per_90,
        total_clean_sheets,
        clean_sheets_per_90,
        total_penalties_saved,
        penalties_order,
        corners_and_indirect_freekicks_order,
        direct_freekicks_order,
        transfers_in,
        transfers_out,
        total_yellow_cards,
        total_red_cards,
        overall_selected_by_percent,
        overall_selected_rank,
        in_dreamteam,
        dreamteam_count,
        news,
        form,
        overall_ict_index,
        overall_influence,
        overall_creativity,
        overall_threat,
        points_per_game_rank,
        now_cost_rank,
        form_rank,
        ict_index_rank,
        influence_rank,
        creativity_rank,
        threat_rank
    from
        {{ ref('stg_players') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_key,
    {{ dbt_utils.generate_surrogate_key(['team_id']) }} as team_key,
    player_id,
    team_id,
    web_name,
    first_name,
    second_name,
    total_points,
    points_per_game,
    total_bonus,
    now_cost,
    status,
    total_goals_scored,
    total_assists,
    total_goals_conceded,
    total_own_goals,
    total_minutes,
    position,
    total_penalties,
    total_starts,
    starts_per_90,
    saves_per_90,
    goals_conceded_per_90,
    total_expected_goals,
    total_expected_assists,
    total_expected_goal_involvements,
    total_expected_goals_conceded,
    expected_goal_involvements_per_90,
    expected_goals_per_90,
    expected_assists_per_90,
    expected_goals_conceded_per_90,
    total_clean_sheets,
    clean_sheets_per_90,
    total_penalties_saved,
    penalties_order,
    corners_and_indirect_freekicks_order,
    direct_freekicks_order,
    transfers_in,
    transfers_out,
    total_yellow_cards,
    total_red_cards,
    overall_selected_by_percent,
    overall_selected_rank,
    in_dreamteam,
    dreamteam_count,
    news,
    form,
    overall_ict_index,
    overall_influence,
    overall_creativity,
    overall_threat,
    points_per_game_rank,
    now_cost_rank,
    form_rank,
    ict_index_rank,
    influence_rank,
    creativity_rank,
    threat_rank
from
    stg_players
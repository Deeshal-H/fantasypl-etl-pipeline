with fact_player_stats as (
    select
        player_stats_key,
        player_key,
        gameweek_key,
        fixture_key,
        opponent_team_key,
        home_team_score,
        away_team_score,
        points,
        bonus,
        cost,
        goals_scored,
        assists,
        goals_conceded,
        clean_sheets,
        own_goals,
        penalties_missed,
        saves,
        penalties_saved,
        expected_goal_involvements,
        expected_goals,
        expected_assists,
        expected_goals_conceded,
        was_home,
        starts,
        minutes,
        yellow_cards,
        red_cards,
        kickoff_time,
        bps,
        transfers_in_round,
        transfers_out_round,
        transfers_balance,
        selected,
        influence,
        creativity,
        threat,
        ict_index
    from
        {{ ref('fact_player_stats') }}
),

dim_players as (
    select
        player_key,
        team_key,
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
        {{ ref('dim_players') }}
),

dim_teams as (
    select
        team_key,
        team_id,
        team_name,
        short_name
    from
        {{ ref('dim_teams') }}
)

select
    {{ dbt_utils.star(from=ref('fact_player_stats'), relation_alias='fact_player_stats', except=["player_stats_key","player_key", "gameweek_key",
        "fixture_key", "opponent_team_key", "player_id", "gameweek_id", "fixture_id", "opponent_team_id"]) }},
    {{ dbt_utils.star(from=ref('dim_players'), relation_alias='dim_players', except=['player_key', 'team_key','team_id']) }},
    {{ dbt_utils.star(from=ref('dim_teams'), relation_alias='dim_teams', except=['team_key', 'team_id']) }},
    {{ dbt_utils.star(from=ref('dim_teams'), relation_alias='opponent', prefix='opponent_', except=['team_key', 'team_id']) }}
from fact_player_stats
inner join dim_players on fact_player_stats.player_key = dim_players.player_key
inner join dim_teams on dim_teams.team_key = dim_players.team_key
inner join dim_teams as opponent on opponent.team_key = fact_player_stats.opponent_team_key

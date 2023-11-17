with stg_player_stats as (
    select
        player_id,
        gameweek_id,
        fixture_id,
        opponent_team_id,
        home_team_score,
        away_team_score,
        total_points,
        bonus,
        value,
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
        transfers_in,
        transfers_out,
        transfers_balance,
        selected,
        influence,
        creativity,
        threat,
        ict_index
    from
        {{ ref('stg_player_stats') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['player_id', 'fixture_id']) }} as player_stats_key,
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_key,
    {{ dbt_utils.generate_surrogate_key(['gameweek_id']) }} as gameweek_key,
    {{ dbt_utils.generate_surrogate_key(['fixture_id']) }} as fixture_key,
    {{ dbt_utils.generate_surrogate_key(['opponent_team_id']) }} as opponent_team_key,
    player_id,
    gameweek_id,
    fixture_id,
    opponent_team_id,
    home_team_score,
    away_team_score,
    total_points,
    bonus,
    value,
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
    transfers_in,
    transfers_out,
    transfers_balance,
    selected,
    influence,
    creativity,
    threat,
    ict_index
from
    stg_player_stats
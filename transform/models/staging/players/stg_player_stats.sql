select
    element as player_id,
    round as gameweek_id,
    fixture as fixture_id,
    opponent_team as opponent_team_id,
    team_h_score as home_team_score,
    team_a_score as away_team_score,
    total_points as points,
    bonus,
    value as cost,
    goals_scored,
    assists,
    goals_conceded,
    clean_sheets,
    own_goals,
    penalties_missed,
    saves,
    penalties_saved,
    cast(expected_goal_involvements as float) as expected_goal_involvements,
    cast(expected_goals as float) as expected_goals,
    cast(expected_assists as float) as expected_assists,
    cast(expected_goals_conceded as float) as expected_goals_conceded,
    was_home,
    starts,
    minutes,
    yellow_cards,
    red_cards,
    cast(kickoff_time as datetime) as kickoff_time,
    bps,
    transfers_in as transfers_in_round,
    transfers_out as transfers_out_round,
    transfers_balance,
    selected,
    cast(influence as float) as influence,
    cast(creativity as float) as creativity,
    cast(threat as float) as threat,
    cast(ict_index as float) as ict_index
from
    {{ source('fantasypl_raw', 'player_stats') }}
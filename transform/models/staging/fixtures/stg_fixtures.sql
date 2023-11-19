select
    id as fixture_id,
    event as gameweek_id,
    team_h as home_team_id,
    team_a as away_team_id,
    team_h_score as home_team_score,
    team_a_score as away_team_score,
    cast(kickoff_time as datetime) as kickoff_time,
    started,
    finished,
    team_a_difficulty as away_team_difficulty,
    team_h_difficulty as home_team_difficulty
from
    {{ source('fantasypl_raw', 'fixtures') }}
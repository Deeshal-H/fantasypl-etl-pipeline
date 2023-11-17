with stg_fixtures as (
    select
        fixture_id,
        gameweek_id,
        home_team_id,
        away_team_id,
        home_team_score,
        away_team_score,
        kickoff_time,
        started,
        finished,
        away_team_difficulty,
        home_team_difficulty
    from
        {{ ref('stg_fixtures') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['fixture_id']) }} as fixture_key,
    {{ dbt_utils.generate_surrogate_key(['gameweek_id']) }} as gameweek_key,
    {{ dbt_utils.generate_surrogate_key(['home_team_id']) }} as home_team_key,
    {{ dbt_utils.generate_surrogate_key(['away_team_id']) }} as away_team_key,
    fixture_id,
    gameweek_id,
    home_team_id,
    away_team_id,
    home_team_score,
    away_team_score,
    kickoff_time,
    started,
    finished,
    away_team_difficulty,
    home_team_difficulty
from stg_fixtures
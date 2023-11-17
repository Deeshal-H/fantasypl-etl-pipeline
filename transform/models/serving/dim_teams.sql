with stg_teams as (
    select
        team_id,
        team_name,
        short_name
    from
        {{ ref('stg_teams') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['team_id']) }} as team_key,
    team_id,
    team_name,
    short_name
from
    stg_teams
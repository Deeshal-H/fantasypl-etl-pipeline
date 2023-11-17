select
    id as team_id,
    name as team_name,
    short_name
from
    {{ source('fantasypl_raw', 'teams') }}
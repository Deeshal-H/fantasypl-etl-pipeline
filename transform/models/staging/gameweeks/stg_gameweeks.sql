select
    id as gameweek_id,
    name as gameweek_name,
    deadline_time,
    finished,
    is_current,
    top_element as top_player_id,
    "top_element_info.points" as top_player_points,
    most_captained as most_captained_player_id,
    most_vice_captained as most_vice_captained_player_id,
    most_transferred_in as most_transferred_in_player_id,
    most_selected as most_selected_player_id,
    average_entry_score,
    highest_score,
    transfers_made
from {{ source('fantasypl_raw', 'gameweeks') }}


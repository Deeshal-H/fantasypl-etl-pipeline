with stg_gameweeks as (
    select
        gameweek_id,
        gameweek_name,
        deadline_time,
        finished,
        is_current,
        top_player_id,
        top_player_points,
        most_captained_player_id,
        most_vice_captained_player_id,
        most_transferred_in_player_id,
        most_selected_player_id,
        average_entry_score,
        highest_score,
        transfers_made
    from {{ ref('stg_gameweeks') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['gameweek_id']) }} as gameweek_key,
    {{ dbt_utils.generate_surrogate_key(['top_player_id']) }} as top_player_key,
    {{ dbt_utils.generate_surrogate_key(['most_captained_player_id']) }} as most_captained_player_key,
    {{ dbt_utils.generate_surrogate_key(['most_vice_captained_player_id']) }} as most_vice_captained_player_key,
    {{ dbt_utils.generate_surrogate_key(['most_transferred_in_player_id']) }} as most_transferred_in_player_key,
    {{ dbt_utils.generate_surrogate_key(['most_selected_player_id']) }} as most_selected_player_key,
    gameweek_id,
    gameweek_name,
    deadline_time,
    finished,
    is_current,
    top_player_id,
    top_player_points,
    most_captained_player_id,
    most_vice_captained_player_id,
    most_transferred_in_player_id,
    most_selected_player_id,
    average_entry_score,
    highest_score,
    transfers_made
from
    stg_gameweeks
WITH cte_players_xG AS (
    SELECT
        player_key,
        player_id,
        web_name,
        SUM(expected_goal_involvements) AS xGI,
        SUM(expected_goals_conceded) AS xGC,
        position,
        team_key,
        team_id,
        team_name
    FROM
    (
        SELECT
            ROW_NUMBER() OVER (partition BY p.player_id ORDER BY kickoff_time DESC) AS row_num,
            p.player_key,
            p.player_id,
            web_name,
            ps.expected_goal_involvements,
            ps.expected_goals_conceded,
            ps.kickoff_time,
            position,
            ps.yellow_cards,
            ps.red_cards,
            p.team_key,
            p.team_id,
            t.team_name
        FROM {{ ref('fact_player_stats') }} AS ps
        INNER JOIN {{ ref('dim_players') }} AS p ON ps.player_key = p.player_key
        INNER JOIN {{ ref('dim_teams') }} AS t ON t.team_key = p.team_key
        WHERE ps.minutes > 0
    ) AS last_5_games
    WHERE last_5_games.row_num <= 5
    GROUP BY
        player_key,
        player_id,
        web_name,
        position,
        team_key,
        team_id,
        team_name
),

cte_teams_with_xG AS (
    SELECT
        SUM(xGI) AS cumm_xGI,
        SUM(xGC) AS cumm_xGC,
        team_key,
        team_id,
        team_name
    FROM cte_players_xG
    GROUP BY
        team_key,
        team_id,
        team_name
    ORDER BY cumm_xGC DESC
)

SELECT
    player_id,
    web_name,
    team_name,
    opponent_id,
    opponent,
    xGI,
    xGC, 
    opponent_cumm_xGI,
    opponent_cumm_xGC,
    kickoff_time,
    position,
    gameweek_id,
    gameweek_order
FROM
(
    SELECT
        player_id,
        web_name,
        team_name,
        opponent_id,
        opponent,
        CAST(xGI AS FLOAT) AS xGI,
        CAST(xGC AS FLOAT) AS xGC, 
        opponent_cumm_xGI,
        opponent_cumm_xGC,
        kickoff_time,
        position,
        gameweek_id,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY kickoff_time) AS gameweek_order
    FROM
    (
        SELECT
            player_key,
            player_id,
            web_name,
            xG.team_name,
            t_a.team_key AS opponent_team_key,
            t_a.team_id AS opponent_id,
            t_a.team_name AS opponent,
            xGI,
            xGC,
            t_a.cumm_xGI AS opponent_cumm_xGI,
            t_a.cumm_xGC AS opponent_cumm_xGC,
            kickoff_time,
            position,
            gameweek_id
        FROM cte_players_xg AS xG
        INNER JOIN {{ ref('dim_fixtures') }} AS f_h ON xG.team_key = f_h.home_team_key
        INNER JOIN cte_teams_with_xG AS t_a ON t_a.team_key = f_h.away_team_key
        WHERE f_h.kickoff_time > GETDATE()
        
        UNION
        
        SELECT
            player_key,
            player_id,
            web_name,
            xG.team_name,
            t_h.team_key AS opponent_team_key,
            t_h.team_id AS opponent_id,
            t_h.team_name AS opponent,
            xGI,
            xGC,
            t_h.cumm_xGI AS opponent_cumm_xGI,
            t_h.cumm_xGC AS opponent_cumm_xGC,
            kickoff_time,
            position,
            gameweek_id
        FROM cte_players_xg AS xG
        INNER JOIN {{ ref('dim_fixtures') }} AS f_a ON xG.team_key = f_a.away_team_key
        INNER JOIN cte_teams_with_xG AS t_h ON t_h.team_key = f_a.home_team_key
        WHERE f_a.kickoff_time > GETDATE()
    )
    ORDER BY kickoff_time
)
WHERE gameweek_order < 6
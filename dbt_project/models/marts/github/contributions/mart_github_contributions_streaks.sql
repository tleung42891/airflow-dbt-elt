{{ config(
    materialized='table'
) }}

WITH contribution_dates AS (
    SELECT
        username,
        date,
        contribution_count,
        LAG(date) OVER (PARTITION BY username ORDER BY date) AS prev_date,
        date - LAG(date) OVER (PARTITION BY username ORDER BY date) AS days_gap
    FROM {{ ref('stg_github_contributions') }}
    WHERE contribution_count > 0
),

streak_groups AS (
    SELECT
        username,
        date,
        contribution_count,
        SUM(CASE WHEN days_gap > 1 OR prev_date IS NULL THEN 1 ELSE 0 END) 
            OVER (PARTITION BY username ORDER BY date) AS streak_id
    FROM contribution_dates
),

streak_stats AS (
    SELECT
        username,
        streak_id,
        MIN(date) AS streak_start,
        MAX(date) AS streak_end,
        COUNT(*) AS streak_length_days,
        SUM(contribution_count) AS streak_total_contributions,
        AVG(contribution_count) AS streak_avg_contributions
    FROM streak_groups
    GROUP BY username, streak_id
)

SELECT
    username,
    streak_start,
    streak_end,
    streak_length_days,
    streak_total_contributions,
    streak_avg_contributions,
    CASE 
        WHEN streak_end >= CURRENT_DATE - INTERVAL '1 day' THEN 'active'
        ELSE 'ended'
    END AS streak_status
FROM streak_stats
WHERE streak_length_days >= 3  -- Only show streaks of 3+ days
ORDER BY username, streak_start DESC


{{ config(
    materialized='table'
) }}

WITH user_stats AS (
    SELECT
        username,
        COUNT(DISTINCT date) AS total_days,
        SUM(contribution_count) AS total_contributions,
        AVG(contribution_count) AS avg_daily_contributions,
        MAX(contribution_count) AS max_daily_contributions,
        MAX(date) AS last_contribution_date
    FROM {{ ref('stg_github_contributions') }}
    GROUP BY username
),

ranked_users AS (
    SELECT
        username,
        total_days,
        total_contributions,
        avg_daily_contributions,
        max_daily_contributions,
        last_contribution_date,
        RANK() OVER (ORDER BY total_contributions DESC) AS rank_by_total,
        RANK() OVER (ORDER BY avg_daily_contributions DESC) AS rank_by_avg,
        RANK() OVER (ORDER BY total_days DESC) AS rank_by_consistency
    FROM user_stats
)

SELECT
    username,
    total_days AS days_active,
    total_contributions,
    ROUND(avg_daily_contributions, 2) AS avg_daily_contributions,
    max_daily_contributions,
    last_contribution_date,
    rank_by_total,
    rank_by_avg,
    rank_by_consistency,
    -- Overall score (weighted combination)
    ROUND(
        (rank_by_total * 0.5 + rank_by_avg * 0.3 + rank_by_consistency * 0.2),
        2
    ) AS overall_score
FROM ranked_users
ORDER BY overall_score ASC, total_contributions DESC


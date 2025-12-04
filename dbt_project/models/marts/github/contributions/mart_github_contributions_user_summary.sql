{{ config(
    materialized='table'
) }}

SELECT
    username,
    COUNT(DISTINCT date) AS total_days_with_contributions,
    MIN(date) AS first_contribution_date,
    MAX(date) AS last_contribution_date,
    SUM(contribution_count) AS total_contributions,
    AVG(contribution_count) AS avg_daily_contributions,
    MAX(contribution_count) AS max_daily_contributions,
    STDDEV(contribution_count) AS stddev_daily_contributions,
    -- Calculate total days in range
    (MAX(date) - MIN(date) + 1) AS total_days_in_range,
    -- Calculate activity rate
    ROUND(
        COUNT(DISTINCT date)::NUMERIC / 
        NULLIF((MAX(date) - MIN(date) + 1), 0) * 100,
        2
    ) AS overall_activity_rate_percent,
    -- Current streak (days since last contribution)
    CURRENT_DATE - MAX(date) AS days_since_last_contribution
FROM {{ ref('stg_github_contributions') }}
GROUP BY username
ORDER BY total_contributions DESC


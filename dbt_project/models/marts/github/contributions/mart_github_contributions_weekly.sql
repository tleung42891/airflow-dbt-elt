{{ config(
    materialized='table'
) }}

SELECT
    username,
    DATE_TRUNC('week', date)::DATE AS week_start,
    COUNT(DISTINCT date) AS days_with_contributions,
    SUM(contribution_count) AS total_contributions,
    AVG(contribution_count) AS avg_daily_contributions,
    MAX(contribution_count) AS max_daily_contributions,
    MIN(contribution_count) AS min_daily_contributions
FROM {{ ref('stg_github_contributions') }}
GROUP BY username, DATE_TRUNC('week', date)::DATE
ORDER BY username, week_start DESC


{{ config(
    materialized='table'
) }}

SELECT
    username,
    date,
    contribution_count,
    SUM(contribution_count) OVER (
        PARTITION BY username 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS contributions_last_7_days,
    AVG(contribution_count) OVER (
        PARTITION BY username 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS avg_contributions_last_7_days,
    SUM(contribution_count) OVER (
        PARTITION BY username 
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS contributions_last_30_days,
    AVG(contribution_count) OVER (
        PARTITION BY username 
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_contributions_last_30_days
FROM {{ ref('stg_github_contributions') }}
WHERE date >= CURRENT_DATE - INTERVAL '90 days'  -- Only recent data for performance
ORDER BY username, date DESC


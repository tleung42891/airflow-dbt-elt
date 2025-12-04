{{ config(
    materialized='table'
) }}

WITH monthly_data AS (
    SELECT
        username,
        date,
        contribution_count,
        DATE_TRUNC('month', date)::DATE AS month_start
    FROM {{ ref('stg_github_contributions') }}
)

SELECT
    username,
    month_start,
    TO_CHAR(month_start, 'YYYY-MM') AS month_label,
    COUNT(DISTINCT date) AS days_with_contributions,
    SUM(contribution_count) AS total_contributions,
    AVG(contribution_count) AS avg_daily_contributions,
    MAX(contribution_count) AS max_daily_contributions,
    MIN(contribution_count) AS min_daily_contributions,
    -- Calculate contribution rate (days with contributions / total days in month)
    ROUND(
        COUNT(DISTINCT date)::NUMERIC / 
        EXTRACT(DAY FROM (month_start + INTERVAL '1 month - 1 day'))::NUMERIC * 100,
        2
    ) AS contribution_rate_percent
FROM monthly_data
GROUP BY username, month_start
ORDER BY username, month_start DESC


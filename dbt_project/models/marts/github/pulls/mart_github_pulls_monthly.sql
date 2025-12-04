{{ config(
    materialized='table'
) }}

WITH monthly_data AS (
    SELECT
        user_login,
        repo_name,
        state,
        created_at,
        merged_at,
        DATE_TRUNC('month', created_at)::DATE AS month_start,
        CASE 
            WHEN merged_at IS NOT NULL THEN 
                EXTRACT(EPOCH FROM (merged_at - created_at)) / 86400
            ELSE NULL
        END AS days_to_merge
    FROM {{ ref('stg_github_pulls') }}
)

SELECT
    month_start,
    TO_CHAR(month_start, 'YYYY-MM') AS month_label,
    COUNT(*) AS total_pulls,
    COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END) AS merged_pulls,
    COUNT(CASE WHEN state = 'closed' AND merged_at IS NULL THEN 1 END) AS closed_not_merged_pulls,
    COUNT(CASE WHEN state = 'open' THEN 1 END) AS open_pulls,
    ROUND(
        COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END)::NUMERIC / 
        NULLIF(COUNT(CASE WHEN state = 'closed' THEN 1 END), 0) * 100,
        2
    ) AS merge_rate_percent,
    COUNT(DISTINCT user_login) AS unique_contributors,
    COUNT(DISTINCT repo_name) AS unique_repos,
    ROUND(AVG(days_to_merge), 2) AS avg_days_to_merge
FROM monthly_data
GROUP BY month_start
ORDER BY month_start DESC


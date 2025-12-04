{{ config(
    materialized='table'
) }}

SELECT
    user_login,
    COUNT(*) AS total_pulls,
    COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END) AS merged_pulls,
    COUNT(CASE WHEN state = 'closed' AND merged_at IS NULL THEN 1 END) AS closed_not_merged_pulls,
    COUNT(CASE WHEN state = 'open' THEN 1 END) AS open_pulls,
    ROUND(
        COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END)::NUMERIC / 
        NULLIF(COUNT(CASE WHEN state = 'closed' THEN 1 END), 0) * 100,
        2
    ) AS merge_rate_percent,
    MIN(created_at) AS first_pr_date,
    MAX(created_at) AS last_pr_date,
    COUNT(DISTINCT repo_name) AS repos_contributed_to,
    -- Average time to merge (in days)
    ROUND(
        AVG(
            CASE 
                WHEN merged_at IS NOT NULL THEN 
                    EXTRACT(EPOCH FROM (merged_at - created_at)) / 86400
                ELSE NULL
            END
        )::NUMERIC,
        2
    ) AS avg_days_to_merge
FROM {{ ref('stg_github_pulls') }}
GROUP BY user_login
ORDER BY total_pulls DESC


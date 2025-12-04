{{ config(
    materialized='table'
) }}

SELECT
    repo_name,
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
    MIN(created_at) AS first_pr_date,
    MAX(created_at) AS last_pr_date,
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
    ) AS avg_days_to_merge,
    -- Median time to merge (approximate using percentile)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY 
            CASE 
                WHEN merged_at IS NOT NULL THEN 
                    EXTRACT(EPOCH FROM (merged_at - created_at)) / 86400
                ELSE NULL
            END
    ) AS median_days_to_merge
FROM {{ ref('stg_github_pulls') }}
GROUP BY repo_name
ORDER BY total_pulls DESC


{{ config(
    materialized='table'
) }}

SELECT
    user_login,
    repo_name,
    pr_id,
    state,
    created_at,
    merged_at,
    CASE 
        WHEN merged_at IS NOT NULL THEN 
            EXTRACT(EPOCH FROM (merged_at - created_at)) / 86400
        ELSE NULL
    END AS days_to_merge,
    -- Rolling counts
    COUNT(*) OVER (
        PARTITION BY user_login 
        ORDER BY created_at 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS pulls_last_30_days,
    COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END) OVER (
        PARTITION BY user_login 
        ORDER BY created_at 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS merged_last_30_days
FROM {{ ref('stg_github_pulls') }}
WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'  -- Only recent data for performance
ORDER BY user_login, created_at DESC


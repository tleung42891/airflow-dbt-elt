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
    CASE 
        WHEN merged_at IS NOT NULL THEN 
            EXTRACT(EPOCH FROM (merged_at - created_at)) / 3600
        ELSE NULL
    END AS hours_to_merge,
    CASE 
        WHEN merged_at IS NOT NULL THEN 'merged'
        WHEN state = 'closed' THEN 'closed_not_merged'
        ELSE 'open'
    END AS pr_status
FROM {{ ref('stg_github_pulls') }}
WHERE merged_at IS NOT NULL OR state = 'closed'
ORDER BY created_at DESC


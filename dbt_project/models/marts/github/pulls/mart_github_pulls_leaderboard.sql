{{ config(
    materialized='table'
) }}

WITH user_stats AS (
    SELECT
        user_login,
        COUNT(*) AS total_pulls,
        COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END) AS merged_pulls,
        COUNT(CASE WHEN state = 'open' THEN 1 END) AS open_pulls,
        COUNT(DISTINCT repo_name) AS repos_contributed_to,
        ROUND(
            COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END)::NUMERIC / 
            NULLIF(COUNT(CASE WHEN state = 'closed' THEN 1 END), 0) * 100,
            2
        ) AS merge_rate_percent,
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
),

ranked_users AS (
    SELECT
        user_login,
        total_pulls,
        merged_pulls,
        open_pulls,
        repos_contributed_to,
        merge_rate_percent,
        avg_days_to_merge,
        RANK() OVER (ORDER BY total_pulls DESC) AS rank_by_total,
        RANK() OVER (ORDER BY merged_pulls DESC) AS rank_by_merged,
        RANK() OVER (ORDER BY merge_rate_percent DESC NULLS LAST) AS rank_by_merge_rate,
        RANK() OVER (ORDER BY repos_contributed_to DESC) AS rank_by_repos
    FROM user_stats
)

SELECT
    user_login,
    total_pulls,
    merged_pulls,
    open_pulls,
    repos_contributed_to,
    merge_rate_percent,
    avg_days_to_merge,
    rank_by_total,
    rank_by_merged,
    rank_by_merge_rate,
    rank_by_repos,
    -- Overall score (weighted combination)
    ROUND(
        (rank_by_total * 0.4 + rank_by_merged * 0.3 + rank_by_merge_rate * 0.2 + rank_by_repos * 0.1),
        2
    ) AS overall_score
FROM ranked_users
ORDER BY overall_score ASC, total_pulls DESC


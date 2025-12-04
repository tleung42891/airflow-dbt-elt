{{ config(
    materialized='table'
) }}

WITH monthly_totals AS (
    SELECT
        DATE_TRUNC('month', created_at)::DATE AS month_start,
        COUNT(*) AS monthly_pulls,
        COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END) AS monthly_merged
    FROM {{ ref('stg_github_pulls') }}
    GROUP BY DATE_TRUNC('month', created_at)::DATE
),

monthly_with_lag AS (
    SELECT
        month_start,
        monthly_pulls,
        monthly_merged,
        LAG(monthly_pulls) OVER (ORDER BY month_start) AS prev_month_pulls,
        LAG(monthly_merged) OVER (ORDER BY month_start) AS prev_month_merged
    FROM monthly_totals
)

SELECT
    month_start,
    TO_CHAR(month_start, 'YYYY-MM') AS month_label,
    monthly_pulls,
    monthly_merged,
    prev_month_pulls,
    prev_month_merged,
    monthly_pulls - COALESCE(prev_month_pulls, 0) AS month_over_month_change,
    monthly_merged - COALESCE(prev_month_merged, 0) AS merged_month_over_month_change,
    CASE 
        WHEN prev_month_pulls > 0 THEN
            ROUND(
                ((monthly_pulls - prev_month_pulls)::NUMERIC / prev_month_pulls) * 100,
                2
            )
        ELSE NULL
    END AS month_over_month_pct_change,
    -- Calculate 3-month moving average
    ROUND(
        AVG(monthly_pulls) OVER (
            ORDER BY month_start 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS three_month_avg_pulls,
    ROUND(
        AVG(monthly_merged) OVER (
            ORDER BY month_start 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS three_month_avg_merged
FROM monthly_with_lag
ORDER BY month_start DESC


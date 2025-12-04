{{ config(
    materialized='table'
) }}

WITH monthly_totals AS (
    SELECT
        username,
        DATE_TRUNC('month', date)::DATE AS month_start,
        SUM(contribution_count) AS monthly_contributions
    FROM {{ ref('stg_github_contributions') }}
    GROUP BY username, DATE_TRUNC('month', date)::DATE
),

monthly_with_lag AS (
    SELECT
        username,
        month_start,
        monthly_contributions,
        LAG(monthly_contributions) OVER (PARTITION BY username ORDER BY month_start) AS prev_month_contributions,
        LAG(month_start) OVER (PARTITION BY username ORDER BY month_start) AS prev_month_start
    FROM monthly_totals
)

SELECT
    username,
    month_start,
    monthly_contributions,
    prev_month_contributions,
    monthly_contributions - COALESCE(prev_month_contributions, 0) AS month_over_month_change,
    CASE 
        WHEN prev_month_contributions > 0 THEN
            ROUND(
                ((monthly_contributions - prev_month_contributions)::NUMERIC / prev_month_contributions) * 100,
                2
            )
        ELSE NULL
    END AS month_over_month_pct_change,
    -- Calculate 3-month moving average
    ROUND(
        AVG(monthly_contributions) OVER (
            PARTITION BY username 
            ORDER BY month_start 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS three_month_avg
FROM monthly_with_lag
ORDER BY username, month_start DESC


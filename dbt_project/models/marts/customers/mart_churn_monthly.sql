{{ config(
    materialized='table'
) }}

WITH churned_customers AS (
    SELECT
        month_start,
        region,
        COUNT(DISTINCT customer_id) AS churned_customers
    FROM {{ ref('int_customer_status_changes') }}
    WHERE status = 'canceled'
      AND is_churned_this_month = TRUE
    GROUP BY 1,2
),

previous_month_active AS (
    SELECT
        month_start,
        region,
        COUNT(DISTINCT customer_id) AS active_customers_prev_month
    FROM {{ ref('int_customer_status_changes') }}
    WHERE is_active_in_month = TRUE
    GROUP BY 1,2
)

SELECT
    cc.month_start,
    TO_CHAR(cc.month_start, 'YYYY-MM') AS month_label,
    cc.region,
    cc.churned_customers,
    ROUND(
        cc.churned_customers::NUMERIC / 
        NULLIF(
            pm.active_customers_prev_month,
            0
        ) * 100,
        2
    ) AS churn_rate_percent
FROM churned_customers cc
LEFT JOIN previous_month_active pm
    ON cc.region = pm.region
    AND pm.month_start = DATE_TRUNC('month', cc.month_start - INTERVAL '1 month')::DATE
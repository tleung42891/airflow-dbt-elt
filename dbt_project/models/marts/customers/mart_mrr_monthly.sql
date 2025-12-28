{{ config(
    materialized='table'
) }}

-- MRR calculated from actual subscription and plan pricing data
WITH subscription_mrr AS (
    SELECT
        month_start,
        region,
        SUM(mrr) AS total_mrr,
        COUNT(DISTINCT customer_id) AS active_customers,
        COUNT(DISTINCT subscription_id) AS active_subscriptions,
        -- New MRR from subscriptions that started this month
        SUM(
            CASE 
                WHEN DATE_TRUNC('month', start_date)::DATE = month_start THEN mrr 
                ELSE 0 
            END) AS new_mrr,
        -- Churned MRR from subscriptions that ended this month
        SUM(
            CASE 
                WHEN is_churned_this_month THEN mrr 
                ELSE 0 
            END) AS churned_mrr
    FROM {{ ref('int_subscription_snapshots') }}
    WHERE is_active_in_month = TRUE
    GROUP BY 1,2
)

SELECT
    sm.month_start,
    TO_CHAR(sm.month_start, 'YYYY-MM') AS month_label,
    sm.region,
    sm.active_customers,
    sm.active_subscriptions,
    sm.total_mrr,
    sm.new_mrr,
    sm.churned_mrr,
    -- Average MRR per customer
    ROUND(sm.total_mrr / NULLIF(sm.active_customers, 0),2) AS avg_mrr_per_customer
FROM subscription_mrr sm
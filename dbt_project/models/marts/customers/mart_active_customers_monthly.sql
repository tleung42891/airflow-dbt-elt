{{ config(
    materialized='table'
) }}

SELECT
    month_start,
    TO_CHAR(month_start, 'YYYY-MM') AS month_label,
    region,
    COUNT(DISTINCT customer_id) AS active_customers,
    COUNT(
        DISTINCT CASE 
            WHEN DATE_TRUNC('month', signup_date)::DATE = month_start 
            THEN id 
        END) AS new_customers,
    COUNT(
        DISTINCT CASE 
            WHEN is_churned_this_month 
            THEN id 
        END) AS churned_customers
FROM {{ ref('int_subscription_snapshots') }}
WHERE is_active_in_month = TRUE
GROUP BY 1,2,3
{{ config(
    materialized='table'
) }}

WITH customer_subscription_activity AS (
    SELECT
        customer_id,
        month_start,
        region,
        MAX(is_active_in_month)::BOOLEAN AS has_active_subscription,
        MAX(is_churned_this_month)::BOOLEAN AS has_churned_subscription
    FROM {{ ref('int_subscription_snapshots') }}
    GROUP BY 1,2,3
),

customers_base AS (
    SELECT
        id AS customer_id,
        signup_date,
        region,
        status,
        DATE_TRUNC('month', signup_date)::DATE AS signup_month
    FROM {{ ref('stg_customers') }}
),

monthly_snapshots AS (
    SELECT
        cb.customer_id,
        cb.signup_date,
        cb.signup_month,
        cb.region,
        cb.status,
        DATE_TRUNC('month', month_series)::DATE AS month_start
    FROM customers_base cb
    CROSS JOIN LATERAL generate_series(
        cb.signup_month,
        DATE_TRUNC('month', CURRENT_DATE)::DATE,
        '1 month'::INTERVAL
    ) AS month_series
)

SELECT
    ms.customer_id,
    ms.signup_date,
    ms.signup_month,
    ms.region,
    ms.status,
    ms.month_start,
    -- Customer is active if they have an active subscription OR (status is active AND month is >= signup month)
    COALESCE(csa.has_active_subscription, FALSE) OR (ms.status = 'active' AND ms.month_start >= ms.signup_month) AS is_active_in_month,
    -- Customer churned if subscription churned OR (status is canceled and current month)
    COALESCE(csa.has_churned_subscription, FALSE) OR (ms.status = 'canceled' AND ms.month_start = DATE_TRUNC('month', CURRENT_DATE)::DATE) AS is_churned_this_month
FROM monthly_snapshots ms
LEFT JOIN customer_subscription_activity csa
    ON ms.customer_id = csa.customer_id
    AND ms.month_start = csa.month_start

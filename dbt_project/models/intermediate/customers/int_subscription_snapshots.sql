{{ config(
    materialized='table'
) }}


WITH subscriptions_with_plans AS (
    SELECT
        subscription.id AS subscription_id,
        customer.id AS customer_id,
        subscription.plan_id,
        subscription.start_date,
        subscription.end_date,
        -- Derive status from end_date: null = active, not null = canceled/expired
        CASE 
            WHEN subscription.end_date IS NULL THEN 'active'
            ELSE 'canceled'
        END AS status,
        plan.monthly_price,
        customer.region
    FROM {{ ref('stg_subscriptions') }} subscription
    LEFT JOIN {{ ref('stg_plans') }} plan ON subscription.plan_id = plan.id
    LEFT JOIN {{ ref('stg_customers') }} customer ON subscription.customer_id = customer.id
),

subscription_months AS (
    -- Generate all months from subscription start to end (or current month if active)
    SELECT
        swp.subscription_id,
        swp.customer_id,
        swp.plan_id,
        swp.start_date,
        swp.end_date,
        swp.status,
        swp.monthly_price,
        swp.region,
        DATE_TRUNC('month', month_series)::DATE AS month_start,
        -- Determine if subscription is active in this month (active if month is between start and end, or end_date is null)
        CASE 
            WHEN DATE_TRUNC('month', month_series)::DATE >= DATE_TRUNC('month', swp.start_date)::DATE
                 AND (swp.end_date IS NULL 
                      OR DATE_TRUNC('month', month_series)::DATE <= DATE_TRUNC('month', swp.end_date)::DATE)
            THEN TRUE 
            ELSE FALSE 
        END AS is_active_in_month,
        -- Determine if subscription churned in this month (churned if end_date exists and month matches end_date month)
        CASE 
            WHEN swp.end_date IS NOT NULL
                 AND DATE_TRUNC('month', month_series)::DATE = DATE_TRUNC('month', swp.end_date)::DATE
            THEN TRUE 
            ELSE FALSE 
        END AS is_churned_this_month
    FROM subscriptions_with_plans swp
    CROSS JOIN LATERAL generate_series(
        DATE_TRUNC('month', swp.start_date)::DATE,
        DATE_TRUNC('month', COALESCE(swp.end_date, CURRENT_DATE))::DATE,
        '1 month'::INTERVAL
    ) AS month_series
)

SELECT
    sm.subscription_id,
    sm.customer_id,
    sm.plan_id,
    sm.start_date,
    sm.end_date,
    sm.status,
    sm.monthly_price,
    sm.region,
    sm.month_start,
    sm.is_active_in_month,
    sm.is_churned_this_month,
    -- Calculate MRR for this subscription in this month
    CASE 
        WHEN sm.is_active_in_month THEN sm.monthly_price 
        ELSE 0 
    END AS mrr
FROM subscription_months sm

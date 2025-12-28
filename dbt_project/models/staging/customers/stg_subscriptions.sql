{{ config(
    materialized='table'
) }}

WITH ranked_subscriptions AS (
    SELECT
        id,
        customer_id,
        plan_id,
        start_date::DATE,
        end_date::DATE,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_date DESC) AS row_num
    FROM {{ source('customers', 'raw_subscriptions') }}
)

SELECT
    id,
    customer_id,
    plan_id,
    start_date,
    end_date
FROM ranked_subscriptions
WHERE row_num = 1


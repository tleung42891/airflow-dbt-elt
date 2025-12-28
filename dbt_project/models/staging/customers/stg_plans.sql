{{ config(
    materialized='table'
) }}

WITH ranked_plans AS (
    SELECT
        id,
        plan_name,
        monthly_price::NUMERIC,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY plan_name) AS row_num
    FROM {{ source('customers', 'raw_plans') }}
)

SELECT
    id,
    plan_name,
    monthly_price
FROM ranked_plans
WHERE row_num = 1
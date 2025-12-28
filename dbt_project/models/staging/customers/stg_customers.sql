{{ config(
    materialized='table'
) }}

WITH ranked_customers AS (
    SELECT
        id,
        signup_date::DATE,
        status,
        region,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY signup_date) AS row_num
    FROM {{ source('customers', 'raw_customers') }}
)

SELECT
    id,
    signup_date,
    status,
    region
FROM ranked_customers
WHERE row_num = 1

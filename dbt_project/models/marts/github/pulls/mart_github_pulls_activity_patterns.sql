{{ config(
    materialized='table'
) }}

SELECT
    user_login,
    TO_CHAR(created_at, 'Day') AS day_of_week,
    EXTRACT(DOW FROM created_at) AS day_of_week_num,
    TO_CHAR(created_at, 'Month') AS month_name,
    EXTRACT(MONTH FROM created_at) AS month_num,
    COUNT(*) AS total_pulls,
    COUNT(CASE WHEN state = 'closed' AND merged_at IS NOT NULL THEN 1 END) AS merged_pulls,
    COUNT(CASE WHEN state = 'open' THEN 1 END) AS open_pulls
FROM {{ ref('stg_github_pulls') }}
GROUP BY 
    user_login,
    TO_CHAR(created_at, 'Day'),
    EXTRACT(DOW FROM created_at),
    TO_CHAR(created_at, 'Month'),
    EXTRACT(MONTH FROM created_at)
ORDER BY user_login, day_of_week_num, month_num


{{ config(
    materialized='table'
) }}

SELECT
    username,
    TO_CHAR(date, 'Day') AS day_of_week,
    EXTRACT(DOW FROM date) AS day_of_week_num,
    TO_CHAR(date, 'Month') AS month_name,
    EXTRACT(MONTH FROM date) AS month_num,
    COUNT(DISTINCT date) AS days_with_contributions,
    SUM(contribution_count) AS total_contributions,
    AVG(contribution_count) AS avg_contributions,
    MAX(contribution_count) AS max_contributions
FROM {{ ref('stg_github_contributions') }}
WHERE contribution_count > 0
GROUP BY 
    username,
    TO_CHAR(date, 'Day'),
    EXTRACT(DOW FROM date),
    TO_CHAR(date, 'Month'),
    EXTRACT(MONTH FROM date)
ORDER BY username, day_of_week_num, month_num


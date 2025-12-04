{{ config(
    materialized='table' 
) }}

WITH deduplicated_contributions AS (
    SELECT
        username,
        date,
        contribution_count,
        ROW_NUMBER() OVER (PARTITION BY username, date ORDER BY date) AS row_num
    FROM {{ source('github', 'contributions') }}
)

SELECT *
FROM deduplicated_contributions
WHERE row_num = 1
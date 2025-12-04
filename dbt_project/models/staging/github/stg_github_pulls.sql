{{ config(
    materialized='table' 
) }}

SELECT
    repo_name,
    pr_id,
    state,
    created_at,
    merged_at,
    user_login,
    ROW_NUMBER() OVER (ORDER BY created_at) AS primary_key
FROM {{ source('github', 'pulls') }}
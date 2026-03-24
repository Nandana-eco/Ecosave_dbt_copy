{{ config(
    materialized='view'
    
) }}

SELECT
    user_id,
    lower(firstname) as first_name,
    lower(lastname) as last_name,
    initcap(firstname || ' ' || lastname) as full_name,
    active::boolean as is_active,
    team_id,
    role_plan_id,
    created_date::timestamp as created_date
    
FROM {{ source('users', 'users') }}

{{ config(
    materialized='view'
) }}

SELECT
    userid,
    alias
FROM {{ source('active_users', 'active_users') }}
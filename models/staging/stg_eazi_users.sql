{{ config(
    materialized='view'
) }}

SELECT
    userid,
    alias
FROM {{ source('eazi_users', 'users') }}
{{ config(
    materialized='view'
) }}

SELECT
    *
FROM {{ source('client_meters_field_history', 'client_meters_field_history') }}
{{ config(
    materialized='view'
) }}

SELECT
    *

FROM {{ source('fct_calls', 'fct_calls') }} 
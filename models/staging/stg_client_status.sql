{{ config(
    materialized='view'
) }}

SELECT
    cs.clientid,
    cs.client_status,
    cs.client_status2


FROM {{ source('client_status', 'client_status') }} cs

{{ config(
    materialized='view'
) }}

SELECT
    meterid,
    clientid,
    sold_date,
    meter_status,
    mpan,
    supplier_from,
    supplier_to
FROM {{ source('client_meters', 'client_meters') }}

{{ config(
    materialized='view'
) }}

SELECT
    meterid,
    clientid,
    sold_date,
    meter_status,
    mpan,
    ced,
    meter_type,
    supplier_from,
    supplier_to
FROM {{ source('client_meters', 'client_meters') }}

{{ config(
    materialized='view'
) }}

SELECT
    cm.meterid,
    cm.clientid,
    cm.sold_date,
    cm.meter_status,
    cm.mpan,
    cm.ced,
    cm.meter_type,
    cm.supplier_from,
    cm.supplier_to,
    cm.removed,
    cm.timestamp,
    cm.meter_postcode,
    cm.meter_deal_status 


FROM {{ source('client_meters', 'client_meters') }} cm

{{ config(
    materialized='view'
) }}

SELECT
    l.meterid,
    l.clientid,
    l.sold_date,
    l.meter_status,
    l.mpan,
    l.timestamp,
    l.removed,
    l.ced,
    l.meter_postcode,
    l.meter_type,
    l.meter_deal_status,
    c1.companytitle AS supplier_from_name,
    c2.companytitle AS supplier_to_name

FROM {{ ref('stg_client_meters') }} l

LEFT JOIN {{ ref('stg_company') }} c1
    ON CASE 
           WHEN l.supplier_from ~ '^[0-9]+$' THEN l.supplier_from::SMALLINT
           ELSE NULL
       END = c1.companyid

LEFT JOIN {{ ref('stg_company') }} c2
    ON CASE 
           WHEN l.supplier_to ~ '^[0-9]+$' THEN l.supplier_to::SMALLINT
           ELSE NULL
       END = c2.companyid

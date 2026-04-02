{{ config(
    materialized='view'
) }}

SELECT
    l.meterid,
    l.clientid,
    l.sold_date,
    l.meter_status,
    l.mpan,
    l.supplier_from,
    l.supplier_to,
    c1.companytitle AS supplier_from_name,
    c2.companytitle AS supplier_to_name

FROM {{ ref('stg_client_meters') }} l

LEFT JOIN {{ ref('stg_company') }} c1
    ON l.supplier_from::smallint = c1.companyid

LEFT JOIN {{ ref('stg_company') }} c2
    ON l.supplier_to::smallint = c2.companyid

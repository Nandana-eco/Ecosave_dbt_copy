-- models/stg_leads_with_meters.sql
{{ config(
    materialized='table'
) }}

SELECT
    l.lead_id,
    l.reference_id,
    l.last_result_code,
    s.supplier_name,
    m.meterid,
    m.meter_status,
    m.mpan,
    m.supplier_from_name,
    m.supplier_to_name,
    c.sales_agent_alias,
    c.account_manager_alias  
FROM {{ ref('stg_lead') }} l
LEFT JOIN {{ ref('stg_client_meters_enriched') }} m
    ON NULLIF(l.reference_id, '')::BIGINT = m.clientid
LEFT JOIN {{ ref('stg_suppliers') }} s
   ON NULLIF(l.supplier, '')::BIGINT = s.supplier_id
left join {{ ref('stg_clients_enriched') }} c
    ON NULLIF(l.reference_id, '')::BIGINT = c.clientid
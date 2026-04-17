-- models/stg_leads_with_meters.sql
{{ config(
    materialized='table'
) }}

WITH leads_clean AS (
    SELECT *,
           CASE 
               WHEN reference_id ~ '^[0-9]+$' THEN reference_id::BIGINT
               ELSE NULL
           END AS reference_id_num,
           CASE 
               WHEN supplier ~ '^[0-9]+$' THEN supplier::BIGINT
               ELSE NULL
           END AS supplier_num
    FROM {{ ref('stg_lead') }}
),

base as (
SELECT
    l.lead_id,
    l.reference_id_num,
    l.reference_source,
    l.import_date,
    l.last_result_code,
    li.list_name as current_list,
    scl.total_calls,
    scl.contacted_calls,
    scl.ever_contacted,
    scl.last_call_date,
    m.clientid,
    m.mpan,
    m.supplier_to,
    m.meter_value,
    m.contract_duration,
    DATE(m.row_add_date) as row_add_date,
    c.account_manager_alias AS account_manager,
    m.lead_gen_agent,
    m.closer,
    m.supplier_status,
    m.team,
    CASE 
    WHEN m.supplier_csd::date = '1970-01-01' THEN NULL
    ELSE m.supplier_csd
END AS supplier_csd,
    m.rn as m_rn,
    m.ecosave_status as ecosave_status ,
    s.supplier_name as data_supplier,
    cl.attributed_list_name as attributed_list,
    cme.supplier_from_name,
    cme.meter_type,
    CASE 
    WHEN cme.ced::date = '1970-01-01' THEN NULL
    ELSE cme.ced
END AS ced,
    cme.meter_status,
    cme.timestamp as cme_timestamp 

FROM {{ref('stg_tracker_main_data')}} m
LEFT JOIN leads_clean l 
    ON m.clientid= l.reference_id_num
LEFT JOIN {{ ref('stg_clients_enriched') }} c
    ON l.reference_id_num = c.clientid and c.deactivated = 'N'
LEFT JOIN {{ ref('stg_suppliers') }} s
    ON l.supplier_num = s.supplier_id
left join {{ref('stg_list')}} li
    on l.list_id=li.list_id
left join {{ref('fct_converted_leads')}} cl 
    on l.lead_id=cl.lead_id
left join {{ref('stg_client_meters_enriched')}} cme 
on m.mpan=cme.mpan and m.clientid=cme.clientid 
and cme.removed::int = 0

left join {{ref('stg_contacted_leads')}} scl 
    on l.lead_id=scl.lead_id)

SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY mpan, row_add_date
               ORDER BY cme_timestamp DESC NULLS LAST
           ) AS rn
    FROM base
) x
WHERE rn = 1
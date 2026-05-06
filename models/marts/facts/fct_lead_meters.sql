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
)

SELECT
    l.lead_id,
    l.reference_source,
    li.list_name as current_list,
    lo.list_name as original_list,
    l.import_date,
    l.last_result_code,
    m.ecosave_status as ecosave_status,
    m.mpan as sold_mpan ,
    c.sales_agent_alias as sales_agent,
    c.account_manager_alias as account_manager,
    s.supplier_name as data_supplier,
    cl.attributed_list_name,
    scl.total_calls,
    scl.contacted_calls,
    scl.ever_contacted,
    scl.last_call_date,
    cme.supplier_from_name,
    cme.meter_type,
    cme.mpan as meter_mpan,
    CASE 
    WHEN cme.ced::date = '1970-01-01' THEN NULL
    ELSE cme.ced
END AS ced,
    cme.meter_status,
    CASE 
    WHEN cme.meter_postcode IS NULL THEN NULL
    ELSE 
        LEFT(
            UPPER(REGEXP_REPLACE(cme.meter_postcode, '\s+', '', 'g')),
            LENGTH(REGEXP_REPLACE(cme.meter_postcode, '\s+', '', 'g')) - 3
        )
        || ' ' ||
        RIGHT(
            UPPER(REGEXP_REPLACE(cme.meter_postcode, '\s+', '', 'g')),
            3
        )
END AS meter_postcode
    
FROM leads_clean l                                                                                                                      
LEFT JOIN  {{ref('stg_tracker_main_data')}} m
    ON l.reference_id_num=m.clientid
LEFT JOIN {{ ref('stg_client_meters_enriched') }} cme
on l.reference_id_num=cme.clientid 
and cme.removed::int = 0
    
lEFT JOIN {{ ref('stg_clients_enriched') }} c
    ON l.reference_id_num = c.clientid and c.deactivated = 'N'
LEFT JOIN {{ ref('stg_suppliers') }} s
    ON l.supplier_num = s.supplier_id
left join {{ref('stg_list')}} li
    on l.list_id=li.list_id 
left join {{ref('stg_list')}} lo 
    on l.original_list_id=lo.list_id
left join {{ref('fct_converted_leads')}} cl 
    on l.lead_id=cl.lead_id
left join {{ref('stg_contacted_leads')}} scl 
    on l.lead_id=scl.lead_id



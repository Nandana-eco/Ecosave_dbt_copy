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
    l.reference_id_num,
    l.import_date,
    l.last_result_code,
    li.list_name as current_list,
    m.*,
    s.supplier_name as data_supplier,
    cl.attributed_list_name as attributed_list
FROM leads_clean l
LEFT JOIN {{ ref('stg_tracker_main_data') }} m
    ON l.reference_id_num = m.clientid
LEFT JOIN {{ ref('stg_suppliers') }} s
    ON l.supplier_num = s.supplier_id
left join {{ref('stg_list')}} li
    on l.list_id=li.list_id
left join {{ref('fct_converted_leads')}} cl 
    on l.lead_id=cl.lead_id
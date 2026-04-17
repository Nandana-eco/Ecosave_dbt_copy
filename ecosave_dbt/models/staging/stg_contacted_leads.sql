{{ config(
    materialized='view'
) }}
SELECT
    lead_id,

    COUNT(*) AS total_calls,

   SUM(CASE 
        WHEN is_contacted THEN 1 
        ELSE 0 
    END) AS contacted_calls,

    BOOL_OR(is_contacted) AS ever_contacted,

    MAX(start_at_uk::date) AS last_call_date

FROM {{ source('fct_calls','fct_calls')}}

GROUP BY lead_id 
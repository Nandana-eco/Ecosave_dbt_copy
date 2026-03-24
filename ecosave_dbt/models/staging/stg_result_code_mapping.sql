{{ config(
    materialized='view'
) }}

SELECT
    result_code_id as result_id,
    result_code,
    
    -- Convert Contacted / Not Contacted into boolean
    (contact_bucket = 'Contacted') AS is_contacted,
    
    outcome_bucket,
    description

FROM {{ source('result_code_mapping', 'result_code_mapping') }}
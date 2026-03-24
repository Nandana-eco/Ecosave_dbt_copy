{{ config(
    materialized='view' 
) }}

SELECT
    result_code_id,result_code,description
    
    
FROM {{ source('result_code', 'result_code') }}
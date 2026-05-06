{{ config(
    materialized='view' 
) }}

SELECT
    list_id,list_name,description
    
FROM {{ source('list', 'list') }}

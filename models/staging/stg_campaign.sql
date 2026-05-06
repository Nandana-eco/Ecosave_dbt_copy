{{ config(
    materialized='view' 
) }}

SELECT
    campaign_id,campaign_name,description,first_created
    
    
FROM {{ source('campaign', 'campaign') }}

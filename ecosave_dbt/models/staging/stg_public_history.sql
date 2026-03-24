{{ config(
    materialized='view'
    
    
) }}

SELECT
    history_id,
    lead_id,
    lead_phone_id,
    reference_id,
    list_id,
    campaign_id,
    lead_phone_type,
    result_code,
    result_id,
    user_id,
    start_date_time,end_date_time,
    talk_time as talk_time_ms
FROM {{ source('history', 'history') }}


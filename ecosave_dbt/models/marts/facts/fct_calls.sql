{{ config(materialized='incremental',
    unique_key='history_id')
 }}
with base as (

    select *
    from {{ ref('stg_public_history') }}

),

result_mapping as (

    select
        result_id,
        is_contacted,
        outcome_bucket
    from {{ ref('stg_result_code_mapping') }}

)
select
    b.history_id,
    b.lead_id,
    b.lead_phone_id,
    b.list_id,
    b.campaign_id,
    b.user_id,
    b.reference_id,
    b.result_code,
    b.result_id,
    rm.is_contacted,
    rm.outcome_bucket,

    -- timestamps
    b.start_date_time at time zone 'Europe/London' as start_at_uk,
    b.end_date_time at time zone 'Europe/London' as end_at_uk,

    -- metrics
    round(b.talk_time_ms / 1000.0, 2) as talk_time_seconds,

    -- derived metrics
    case 
        when b.talk_time_ms > 0 then 1 
        else 0 
    end as is_connected

from base b
left join result_mapping rm
    on b.result_id = rm.result_id

{% if is_incremental() %}

WHERE b.start_at_uk > (
    SELECT COALESCE(MAX(b.start_at_uk), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
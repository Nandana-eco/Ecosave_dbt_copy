{{ config(
    materialized='view'
) }}
select
    h.meterid,
    cm.mpan,
    h.value as history_value,
    h.timestamp as history_timestamp
from {{ ref('stg_client_meters_field_history') }} h
left join {{ref('stg_client_meters') }} cm
    on cm.meterID = h.meterID
where h.field = 'ced'
and h.meterID in (
    select meterid
    from {{ ref('stg_all_ids') }}
)
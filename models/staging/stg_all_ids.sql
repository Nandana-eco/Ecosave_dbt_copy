{{ config(
    materialized='table'
) }}
select
    meterid,
    clientid,
    mpan,
    ced
from {{ ref('stg_client_meters')}}
where (clientID, mpan) in (
    select clientID, mpan from {{ ref('stg_tracker_main_data_raw') }}
)
and removed::int = 0
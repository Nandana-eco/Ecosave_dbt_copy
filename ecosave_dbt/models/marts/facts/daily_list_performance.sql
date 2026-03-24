{{ config(
    materialized='incremental',
    unique_key=['date_day', 'list_id']
) }}

with base as (

    select *
    from {{ ref('fct_calls') }}

),

lists as (

    select
        list_id,
        list_name
    from {{ ref('stg_list') }}

),

aggregated as (

    select
        date_trunc('day', start_at_uk) as date_day,
        list_id,

        -- volume
        count(*) as total_calls,
        count(distinct lead_id) as unique_leads,

        -- contact metrics
        sum(case when is_contacted then 1 else 0 end) as contacted_calls,
        round(
            sum(case when is_contacted  then 1 else 0 end) * 1.0
            / nullif(count(*), 0),
            4
        ) as contact_rate,

        -- conversion
        sum(case when outcome_bucket = 'Converted' then 1 else 0 end) as sales,
        round(
            sum(case when outcome_bucket = 'Converted' then 1 else 0 end) * 1.0
            / nullif(sum(case when is_contacted  then 1 else 0 end), 0),
            4
        ) as conversion_rate,

        -- talk time (ONLY contacted calls)
        sum(case when is_contacted then talk_time_seconds else 0 end) 
            as total_talk_time_sec,

        round(
            avg(case when is_contacted then talk_time_seconds end),
            2
        ) as avg_talk_time_sec,

        -- dial efficiency
        round(
            count(*) * 1.0 / nullif(count(distinct lead_id), 0),
            2
        ) as attempts_per_lead

    from base
    group by 1, 2

)

select
    a.date_day,
    a.list_id,
    coalesce(l.list_name, 'Unknown') as list_name,

    a.total_calls,
    a.unique_leads,
    a.contacted_calls,
    a.contact_rate,
    a.sales,
    a.conversion_rate,
    a.total_talk_time_sec,
    a.avg_talk_time_sec,
    a.attempts_per_lead

from aggregated a
left join lists l
    on a.list_id = l.list_id

{% if is_incremental() %}
where a.date_day >= (
    select coalesce(max(date_day), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
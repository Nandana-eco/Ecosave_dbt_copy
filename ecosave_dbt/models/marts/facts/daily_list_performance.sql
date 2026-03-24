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

-- 🔹 Lead-level aggregation
lead_level as (

    select
        date_trunc('day', start_at_uk) as date_day,
        list_id,
        lead_id,

        count(*) as calls_per_lead,

        -- count of unreachable calls
        sum(case when outcome_bucket = 'Unreachable' then 1 else 0 end) as unreachable_calls

    from base
    group by 1,2,3

),

-- 🔹 Dead leads = ALL calls were 'Unreachable'
dead_metrics as (

    select
        date_day,
        list_id,

        sum(
            case 
                when calls_per_lead = unreachable_calls 
                     and calls_per_lead > 0
                then 1 
                else 0 
            end
        ) as dead_leads_unreachable

    from lead_level
    group by 1,2

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
            sum(case when is_contacted then 1 else 0 end) * 1.0
            / nullif(count(*), 0),
            4
        ) as contact_rate,

        -- conversion
        sum(case when outcome_bucket = 'Converted' then 1 else 0 end) as sales,
        round(
            sum(case when outcome_bucket = 'Converted' then 1 else 0 end) * 1.0
            / nullif(sum(case when is_contacted then 1 else 0 end), 0),
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
    a.attempts_per_lead,

    -- 🔹 New metric
    coalesce(d.dead_leads_unreachable, 0) as dead_leads_unreachable

from aggregated a
left join lists l
    on a.list_id = l.list_id
left join dead_metrics d
    on a.date_day = d.date_day
    and a.list_id = d.list_id

{% if is_incremental() %}
where a.date_day >= (
    select coalesce(max(date_day), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
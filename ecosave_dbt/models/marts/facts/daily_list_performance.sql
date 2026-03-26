{{ config(
    materialized='incremental',
    unique_key=['date_day', 'list_id']
) }}

-- 🔹 Base
with base as (
    select *
    from {{ ref('fct_calls') }}
),

-- 🔹 Lists
lists as (
    select
        list_id,
        list_name
    from {{ ref('stg_list') }}
),

-- 🔹 Incremental filter (SAFE)
{% if is_incremental() %}
,last_processed as (
    select coalesce(max(date_day), '1900-01-01'::timestamp) as last_day
    from {{ this }}
),
filtered_base as (
    select b.*
    from base b
    cross join last_processed lp
    where date_trunc('day', b.start_at_uk) >= lp.last_day
),
{% else %}
filtered_base as (
    select * from base
),
{% endif %}

-- 🔹 Lead-level aggregation (CORE)
lead_level as (
    select
        date_trunc('day', start_at_uk) as date_day,
        list_id,
        lead_id,

        count(*) as calls_per_lead,

        -- dead logic
        sum(case when outcome_bucket = 'Unreachable' then 1 else 0 end) as unreachable_calls,

        -- lead-level flags
        max(case when is_contacted then 1 else 0 end) as is_contacted_lead,
        max(case when outcome_bucket = 'Converted' then 1 else 0 end) as is_converted_lead

    from filtered_base
    group by 1,2,3
)

-- 🔹 Dead leads
, dead_metrics as (
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
)

-- 🔹 Call-level aggregation
, call_metrics as (
    select
        date_trunc('day', start_at_uk) as date_day,
        list_id,

        count(*) as total_calls,

        sum(case when is_contacted then 1 else 0 end) as contacted_calls,

        round(
            sum(case when is_contacted then 1 else 0 end) * 1.0
            / nullif(count(*), 0),
            4
        ) as contact_rate_calls,

        sum(case when outcome_bucket = 'Converted' then 1 else 0 end) as sales_calls,

        round(
            sum(case when outcome_bucket = 'Converted' then 1 else 0 end) * 1.0
            / nullif(sum(case when is_contacted then 1 else 0 end), 0),
            4
        ) as conversion_rate_calls,

        sum(case when is_contacted then talk_time_seconds else 0 end) as total_talk_time_sec,

        round(
            avg(case when is_contacted then talk_time_seconds end),
            2
        ) as avg_talk_time_sec

    from filtered_base
    group by 1,2
)

-- 🔹 Lead-level aggregation (final)
, lead_metrics as (
    select
        date_day,
        list_id,

        count(*) as unique_leads,

        sum(is_contacted_lead) as contacted_leads,

        round(
            sum(is_contacted_lead) * 1.0
            / nullif(count(*), 0),
            4
        ) as contact_rate_leads,

        sum(is_converted_lead) as converted_leads,

        round(
            sum(is_converted_lead) * 1.0
            / nullif(sum(is_contacted_lead), 0),
            4
        ) as conversion_rate_leads

    from lead_level
    group by 1,2
)

-- 🔹 Final
select
    c.date_day,
    c.list_id,
    coalesce(l.list_name, 'Unknown') as list_name,

    -- 📞 Call metrics
    c.total_calls,
    c.contacted_calls,
    c.contact_rate_calls,
    c.sales_calls,
    c.conversion_rate_calls,
    c.total_talk_time_sec,
    c.avg_talk_time_sec,

    -- 👤 Lead metrics
    lm.unique_leads,
    lm.contacted_leads,
    lm.contact_rate_leads,
    lm.converted_leads,
    lm.conversion_rate_leads,

    -- 💀 Dead leads
    coalesce(d.dead_leads_unreachable, 0) as dead_leads_unreachable,

    -- efficiency
    round(
        c.total_calls * 1.0 / nullif(lm.unique_leads, 0),
        2
    ) as attempts_per_lead

from call_metrics c
left join lead_metrics lm
    on c.date_day = lm.date_day
    and c.list_id = lm.list_id
left join dead_metrics d
    on c.date_day = d.date_day
    and c.list_id = d.list_id
left join lists l
    on c.list_id = l.list_id
{{ config(
    materialized='incremental',
    unique_key=['week_start', 'list_id']
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
        date_trunc('week', start_at_uk) as week_start,
        list_id,
        lead_id,

        count(*) as calls_per_lead,

        sum(case when outcome_bucket = 'Unreachable' then 1 else 0 end) as unreachable_calls,

        case when sum(case when is_contacted then 1 else 0 end) > 0 then 1 else 0 end as contacted_lead_flag,

        case when sum(case when outcome_bucket = 'Converted' then 1 else 0 end) > 0 then 1 else 0 end as converted_lead_flag

    from base
    group by 1,2,3

),

-- 🔹 Dead leads
dead_metrics as (

    select
        week_start,
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

-- 🔹 Aggregated metrics
aggregated as (

    select
        b.week_start,
        b.list_id,

        -- ✅ total calls (correct grain)
        count(*) as total_calls,

        -- ✅ unique leads (correct grain)
        count(distinct b.lead_id) as unique_leads,

        -- Lead-level metrics (join safely)
        sum(ll.contacted_lead_flag) as contacted_leads,
        round(
            sum(ll.contacted_lead_flag)::numeric 
            / nullif(count(distinct b.lead_id),0),
            4
        ) as contact_rate_lead_level,

        -- Call-level metrics
        sum(case when b.is_contacted then 1 else 0 end) as contacted_calls,
        round(
            sum(case when b.is_contacted then 1 else 0 end)::numeric 
            / nullif(count(*),0),
            4
        ) as contact_rate_call_level,

        -- Lead conversions
        sum(ll.converted_lead_flag) as converted_leads,
        round(
            sum(ll.converted_lead_flag)::numeric 
            / nullif(sum(ll.contacted_lead_flag),0),
            4
        ) as conversion_rate_lead_level,

        -- Call conversions
        sum(case when b.outcome_bucket = 'Converted' then 1 else 0 end) as sales,
        round(
            sum(case when b.outcome_bucket = 'Converted' then 1 else 0 end)::numeric
            / nullif(sum(case when b.is_contacted then 1 else 0 end),0),
            4
        ) as conversion_rate_call_level,

        -- Talk time
        sum(case when b.is_contacted then b.talk_time_seconds else 0 end) as total_talk_time_sec,
        round(avg(case when b.is_contacted then b.talk_time_seconds end),2) as avg_talk_time_sec,

        -- Efficiency
        round(
            count(*)::numeric / nullif(count(distinct b.lead_id),0),
            2
        ) as attempts_per_lead

    from (
        select
            date_trunc('week', start_at_uk) as week_start,
            *
        from base
    ) b

    left join lead_level ll
        on b.lead_id = ll.lead_id
        and b.list_id = ll.list_id
        and b.week_start = ll.week_start

    group by 1,2

),

-- 🔹 Incremental filter (SAFE)
filtered as (

    select *
    from aggregated a

    {% if is_incremental() %}
        where a.week_start > (
            select coalesce(max(week_start), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

)

select
    f.week_start,

    to_char(f.week_start, 'Mon') || '_Week_' ||
    ceil(extract(day from f.week_start)::int / 7.0)::int as week_label,

    f.list_id,
    coalesce(l.list_name, 'Unknown') as list_name,

    f.total_calls,
    f.unique_leads,
    f.contacted_leads,
    f.contact_rate_lead_level,
    f.contacted_calls,
    f.contact_rate_call_level,
    f.converted_leads,
    f.conversion_rate_lead_level,
    f.sales,
    f.conversion_rate_call_level,
    f.total_talk_time_sec,
    f.avg_talk_time_sec,
    f.attempts_per_lead,

    coalesce(d.dead_leads_unreachable, 0) as dead_leads_unreachable

from filtered f
left join lists l
    on f.list_id = l.list_id
left join dead_metrics d
    on f.week_start = d.week_start
    and f.list_id = d.list_id
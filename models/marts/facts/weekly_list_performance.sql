{{ config(
    materialized='incremental',
    unique_key=['week_start', 'list_id']
) }}

with lead_level as (
    select
        date_trunc('week', start_at_uk) as week_start,
        list_id,
        lead_id,

        -- Lead contacted at least once
        max(case when is_contacted then 1 else 0 end) as contacted_flag,

        -- Lead converted at least once
        max(case when outcome_bucket = 'Converted' then 1 else 0 end) as converted_flag

    from {{ ref('fct_calls') }}
    group by 1,2,3
),

call_level as (
    select
        date_trunc('week', start_at_uk) as week_start,
        list_id,

        count(*) as total_calls,
        sum(case when is_contacted then 1 else 0 end) as contacted_calls,
        sum(case when outcome_bucket = 'Converted' then 1 else 0 end) as converted_calls,
        round(
            sum(case when outcome_bucket = 'Converted' then 1 else 0 end)::numeric
            / nullif(sum(case when is_contacted then 1 else 0 end),0),
            4
        ) as call_conversion_rate

    from {{ ref('fct_calls') }}
    group by 1,2
),

lead_summary as (
    select
        week_start,
        list_id,
        count(*) as total_leads,
        sum(contacted_flag) as contacted_leads,
        sum(converted_flag) as converted_leads,
        round(
            sum(converted_flag)::numeric / nullif(sum(contacted_flag),0),
            4
        ) as lead_conversion_rate
    from lead_level
    group by 1,2
),

filtered as (

    select
        l.week_start,
        l.list_id,
        coalesce(s.list_name,'Unknown') as list_name,
        l.total_leads,
        l.contacted_leads,
        l.converted_leads,
        l.lead_conversion_rate,

        c.total_calls,
        c.contacted_calls,
        c.converted_calls,
        c.call_conversion_rate

    from lead_summary l
    left join call_level c
        on l.week_start = c.week_start
        and l.list_id = c.list_id
    left join {{ ref('stg_list') }} s
        on l.list_id = s.list_id


    {% if is_incremental() %}
        where l.week_start > (
            select coalesce(max(week_start), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}

)

select *
from filtered
order by week_start, list_id

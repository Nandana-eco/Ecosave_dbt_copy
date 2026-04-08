{{ config(
    materialized='table',
   
) }}

with calls as (
    select
        lead_id,
        list_id,
        start_at_uk,
        outcome_bucket
    from {{ref('stg_fct_calls')}}
),

-- Step 1: identify only converted calls
converted as (
    select *
    from calls
    where outcome_bucket = 'Converted'
),

-- Step 2: pick the latest converted call per lead
latest_conversion as (
    select *
    from (
        select *,
            row_number() over (
                partition by lead_id
                order by start_at_uk desc
            ) as rn
        from converted
    ) t
    where rn = 1
),

-- Step 3: find the last non-final list before this conversion
previous_non_final as (
    select
        lc.lead_id,
        lc.list_id as conversion_list,
        lc.start_at_uk as conversion_time,
        c.list_id as prev_list_id,
        row_number() over (
            partition by lc.lead_id
            order by c.start_at_uk desc
        ) as rn
    from latest_conversion lc
    left join calls c
        on c.lead_id = lc.lead_id
       and c.start_at_uk < lc.start_at_uk
       and c.list_id not in (123,374,375,376,377)
)

-- Step 4: keep only latest non-final row (or fallback) and join to list table
select
    pnf.lead_id,
    pnf.conversion_list,
    coalesce(pnf.prev_list_id, pnf.conversion_list) as attributed_list_id,
    l.list_name as attributed_list_name
from previous_non_final pnf
left join {{ref('stg_list')}} l 
    on coalesce(pnf.prev_list_id, pnf.conversion_list) = l.list_id
where pnf.rn = 1
--get call hsitory of converted leads and track back the attributed list from that.

with leads as (
    select distinct clientid as ref_id
    from {{ref('stg_tracker_main_data')}}
),

lead_calls as (
    select c.*
    from {{ref('fct_calls')}} c
    join leads l
      on (
        case 
            when c.reference_id ~ '^[0-9]+$' 
                then c.reference_id::int
            when c.reference_id like '%:%'
                then split_part(c.reference_id, ':', 1)::int
            else null
        end
      ) = l.ref_id
),

-- latest call per lead
latest_call as (
    select *
    from (
        select *,
            row_number() over (
                partition by lead_id 
                order by start_at_uk desc
            ) as rn
        from lead_calls
    ) t
    where rn = 1
),

-- previous non-final call ONLY when needed
previous_non_final as (
    select
        lc.lead_id,
        c.list_id as prev_list,
        row_number() over (
            partition by lc.lead_id 
            order by c.start_at_uk desc
        ) as rn
    from latest_call lc
    left join lead_calls c
        on c.lead_id = lc.lead_id
       and c.start_at_uk < lc.start_at_uk
       and c.list_id not in (123,374,375,376,377,525)
)

select
    lc.lead_id,
    lc.list_id as conversion_list,
    coalesce(pnf.prev_list, lc.list_id) as attributed_list_id,
    l.list_name as attributed_list_name
from latest_call lc
left join previous_non_final pnf
    on lc.lead_id = pnf.lead_id
   and pnf.rn = 1
left join public.list l
    on coalesce(pnf.prev_list, lc.list_id) = l.list_id
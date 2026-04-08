with calls as (
    select lead_id,
    list_id,
    start_at_uk,
    outcome_bucket
    from {{ ref('stg_fct_calls') }}
),

converted as (
    select
        c.*,

        max(case 
                when c.list_id not in (123,374,375,376,377) 
                then c.list_id 
            end
        ) over (
            partition by c.lead_id 
            order by c.start_at_uk
            rows between unbounded preceding and 1 preceding
        ) as last_non_final_list

    from calls c
    where c.outcome_bucket = 'Converted'
),

final as (
    select
        lead_id,
        list_id as conversion_list,
        start_at_uk as conversion_time,

        case
            when list_id not in (123,374,375,376,377) 
                then list_id
            else last_non_final_list
        end as attributed_list_id

    from converted
)

select
    f.*,
    sl.list_name as attributed_list_name

from final f
left join {{ ref('stg_list') }} sl
    on f.attributed_list_id = sl.list_id
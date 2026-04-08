{{ config(
    materialized='view'
) }}

SELECT
    l.clientid,
    l.company,
    l.sales_call_complete_agent,
    l.account_manager,
    s1.alias AS sales_agent_alias,
    s2.alias AS account_manager_alias

FROM {{ ref('stg_clients') }} l

LEFT JOIN {{ ref('stg_active_users') }} s1
    ON l.sales_call_complete_agent= s1.userid

LEFT JOIN {{ ref('stg_active_users') }} s2
    ON l.account_manager= s2.userid
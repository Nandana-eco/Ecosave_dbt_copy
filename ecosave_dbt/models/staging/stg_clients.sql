{{ config(
    materialized='view'
) }}

SELECT
    clientid,
    company,
    sales_call_complete_agent,
    account_manager,
    deactivated

FROM {{ source('clients', 'clients') }} 


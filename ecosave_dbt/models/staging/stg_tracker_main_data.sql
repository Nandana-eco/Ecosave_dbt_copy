{{ config(
    materialized='table'
) }}

WITH ranked AS (

    SELECT
        rowid,
        clientid,
        mpan,
        supplier_to,
        meter_value,
        contract_duration,
        row_add_date,
        account_manager,
        lead_gen_agent,
        closer,

        -- ✅ standardised columns
        UPPER(TRIM(ecosave_status)) AS ecosave_status,
        UPPER(TRIM(supplier_status)) AS supplier_status,
        UPPER(TRIM(team)) AS team,

        supplier_csd,
        ROW_NUMBER() OVER (
            PARTITION BY mpan
            ORDER BY row_add_date DESC
        ) AS rn

    FROM {{ source('tracker_main_data', 'tracker_main_data') }}
     

)

SELECT *
FROM ranked
WHERE rn = 1


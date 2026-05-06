{{ config(
    materialized='view'
) }}

SELECT
    companyid,
    companytitle

FROM {{ source('company', 'company') }} 
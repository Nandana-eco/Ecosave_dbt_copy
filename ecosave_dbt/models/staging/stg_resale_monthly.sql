{{ config(
    materialized='table'
) }}

SELECT
    to_date(month, 'Mon-YY') AS month_date,
    total_opportunities,
    sold,
    conversion_rate,
    live,
    lost,
    pending
FROM {{ source('monthly_breakdown', 'monthly_breakdown') }}
{{ config(
    materialized='view'
) }}

WITH ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY mpan
            ORDER BY row_add_date DESC
        ) AS rn

    FROM {{ source('tracker_main_data', 'tracker_main_data') }}

)

SELECT *
FROM ranked
WHERE rn = 1
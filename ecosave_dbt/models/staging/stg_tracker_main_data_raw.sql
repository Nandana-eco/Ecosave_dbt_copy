{{ config(
    materialized='view'
) }}

    SELECT
        *
    FROM {{ source('tracker_main_data', 'tracker_main_data') }}
     where mpan is not null
  and mpan <> ''


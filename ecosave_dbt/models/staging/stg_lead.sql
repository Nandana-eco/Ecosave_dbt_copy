{{ config(
    materialized='view'
) }}

SELECT
    lead_id,
    reference_id,
    name as contact_name,
    name2 as second_name,
    address as address_part_1,
    address2 as address_part_2,
    city,
    state,
    postal_code,
    other_info,
    import_date,
    supplier
FROM {{ source('lead', 'lead') }}



{{ config(
    materialized='incremental',
    unique_key='lead_id'
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
from {{ ref('stg_lead') }}

{% if is_incremental() %}

WHERE import_date > (
    SELECT MAX(import_date) FROM {{ this }}
)

{% endif %}
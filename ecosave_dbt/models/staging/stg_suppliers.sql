{{ config(
    materialized='view' 
) }}

SELECT
    import_supplier_id as supplier_id,
    active as is_active,
    supplier_name
    
    
FROM {{ source('import_supplier', 'import_supplier') }}
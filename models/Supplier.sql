{{ config(
    materialized='incremental',
    unique_key='SupplierName'
) }}

WITH suppliers_in_source AS (
    -- Select distinct suppliers from the package_key_migrate source
    SELECT DISTINCT shipper COLLATE Modern_Spanish_CI_AS AS SupplierName
    FROM {{ source('migration', 'package_key_migrate') }}
),

suppliers_in_target AS (
    -- Select suppliers from the Supplier table
    SELECT SupplierName
    FROM {{ source('migration', 'Supplier') }}
),

-- Find suppliers in source that are missing in the target Supplier table
missing_suppliers AS (
    SELECT s.SupplierName
    FROM suppliers_in_source s
    LEFT JOIN suppliers_in_target t
    ON s.SupplierName = t.SupplierName
    WHERE t.SupplierName IS NULL
)

-- Insert the missing suppliers into the Supplier table
SELECT SupplierName
FROM missing_suppliers


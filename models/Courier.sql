{{ config(
    materialized='incremental',
    unique_key='CourierName'
) }}

WITH couriers_in_source AS (
    -- Select distinct couriers from the package_key_migrate source
    SELECT DISTINCT shipper COLLATE Modern_Spanish_CI_AS AS CourierName
    FROM {{ source('migration', 'package_key_migrate') }}
),

couriers_in_target AS (
    -- Select couriers from the Courier table
    SELECT CourierName
    FROM {{ source('migration', 'Courier') }}
),

-- Find couriers in source that are missing in the target Courier table
missing_couriers AS (
    SELECT s.CourierName
    FROM couriers_in_source s
    LEFT JOIN couriers_in_target t
    ON s.CourierName = t.CourierName
    WHERE t.CourierName IS NULL
)

-- Insert the missing couriers into the Courier table
SELECT CourierName
FROM missing_couriers


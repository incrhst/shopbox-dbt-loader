{{ config(materialized='incremental') }}

{% set colsql = 'COLLATE Modern_Spanish_CI_AS' %}

WITH source AS (
    SELECT * FROM {{ source('migration', 'packageevent_migrate') }}
),

-- Get valid packages from ImportPackage
valid_packages AS (
    SELECT DISTINCT 
        PackageNumber {{ colsql }} as PackageNumber
    FROM {{ source('migration', 'ImportPackage') }}
),

-- First, map event descriptions to status names
status_mapping AS (
    SELECT
        s.package_number {{ colsql }} AS PackageNumber,
        CASE
            WHEN event_description LIKE '%Delivered%' THEN 'Delivered'
            WHEN event_description LIKE '%Detain at Custom%' THEN 'Detained'
            WHEN event_description LIKE '%Transit%' THEN 'In Transit'
            WHEN event_description LIKE '%Received at Ware%' THEN 'ReceivedWarehouse'
            WHEN event_description LIKE '%Received from Shipper%' THEN 'Received'
            WHEN event_description LIKE '%Scanned at Ware%' THEN 'ScannedWarehouse'
            WHEN event_description LIKE '%Released from Custom%' THEN 'Released'
            WHEN event_description LIKE '%Delivery Cancel%' THEN 'Delivery Cancelled'
            WHEN event_description LIKE '%Out for Deliver%' THEN 'Out For Delivery'
            WHEN event_description LIKE '%Scheduled for Deliver%' THEN 'ScheduledDelivery'
            WHEN event_description LIKE '%Returned to Warehouse%' THEN 'Returned'
            ELSE 'Received' -- Default value
        END AS PackageStatusName,
        -- Fixed datetime combination
        CASE
            WHEN event_date IS NOT NULL AND event_time IS NOT NULL
            THEN DATEADD(HOUR, DATEPART(HOUR, event_time),
                 DATEADD(MINUTE, DATEPART(MINUTE, event_time),
                 DATEADD(SECOND, DATEPART(SECOND, event_time), event_date)))
            ELSE event_date
        END AS EventDate
    FROM source s
    -- Only include events for packages that exist in ImportPackage
    INNER JOIN valid_packages vp
        ON vp.PackageNumber = s.package_number {{ colsql }}
),

transformed AS (
    SELECT
        CAST(PackageNumber AS nvarchar(40)) {{ colsql }} AS PackageNumber,
        CAST(PackageStatusName AS nvarchar(20)) AS PackageStatusName,
        EventDate,
        CAST(100 AS int) AS PackageEventUserId,
        CAST(0 AS bit) AS PackageEventInvoiceVerified,
        CAST(NULL AS nvarchar(40)) AS PackageEventInvoiceNumber
    FROM status_mapping
),

-- Add incremental logic if needed
{% if is_incremental() %}
existing_events AS (
    SELECT
        PackageNumber {{ colsql }} AS PackageNumber,  -- Added explicit column name
        PackageStatusName AS PackageStatusName,       -- Added explicit column name
        EventDate AS EventDate                        -- Added explicit column name
    FROM {{ this }}
),

final AS (
    SELECT DISTINCT 
        t.PackageNumber,
        t.PackageStatusName,
        t.EventDate,
        t.PackageEventUserId,
        t.PackageEventInvoiceVerified,
        t.PackageEventInvoiceNumber
    FROM transformed t
    WHERE NOT EXISTS (
        SELECT 1
        FROM existing_events e
        WHERE e.PackageNumber = t.PackageNumber {{ colsql }}
        AND e.PackageStatusName = t.PackageStatusName
        AND e.EventDate = t.EventDate
    )
)
{% else %}
final AS (
    SELECT DISTINCT * FROM transformed
)
{% endif %}

SELECT
    PackageNumber,
    PackageStatusName,
    EventDate,
    PackageEventUserId,
    PackageEventInvoiceVerified,
    PackageEventInvoiceNumber
FROM final
WHERE PackageNumber IS NOT NULL

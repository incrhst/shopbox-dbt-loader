{{ config(
materialized='incremental',
incremental_strategy='merge'
) }}

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
        s.PackageNumber {{ colsql }} AS PackageNumber,
        CASE
            WHEN EventDescription LIKE '%Delivered%' THEN 'Delivered'
            WHEN EventDescription LIKE '%Detain at Custom%' THEN 'Detained'
            WHEN EventDescription LIKE '%Transit%' THEN 'In Transit'
            WHEN EventDescription LIKE '%Received at Ware%' THEN 'ReceivedWarehouse'
            WHEN EventDescription LIKE '%Received from Shipper%' THEN 'Received'
            WHEN EventDescription LIKE '%Scanned at Ware%' THEN 'ScannedWarehouse'
            WHEN EventDescription LIKE '%Released from Custom%' THEN 'Released'
            WHEN EventDescription LIKE '%Delivery Cancel%' THEN 'Delivery Cancelled'
            WHEN EventDescription LIKE '%Out for Deliver%' THEN 'Out For Delivery'
            WHEN EventDescription LIKE '%Scheduled for Deliver%' THEN 'ScheduledDelivery'
            WHEN EventDescription LIKE '%Returned to Warehouse%' THEN 'Returned'
            ELSE 'Received' -- Default value
        END AS PackageStatusName,
        -- Fixed datetime combination
        CASE
            WHEN EventDate IS NOT NULL AND EventTime IS NOT NULL THEN
            CONVERT(DATETIME,
                CONVERT(VARCHAR(10), s.EventDate, 120) + ' ' +
                CONVERT(VARCHAR(8), s.EventTime, 108))
            ELSE
             NULL
        END AS EventDate
    FROM source s
    -- Only include events for packages that exist in ImportPackage
    INNER JOIN valid_packages vp
        ON vp.PackageNumber = s.PackageNumber
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
        PackageNumber,
        PackageStatusName,
        EventDate
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

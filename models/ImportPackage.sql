{{ config(
    materialized='incremental',
    unique_key='PackageNumber',
    on_schema_change='ignore'
) }}

{% set migration_schema = 'migration' %}
{% set colsql = 'COLLATE Modern_Spanish_CI_AS' %}

WITH list_of_existing_package_numbers AS (
    -- Start with ImportPackage to ensure we only process valid packages
    SELECT DISTINCT p.PackageNumber {{ colsql }} as PackageNumber
    FROM {{ source(migration_schema, 'ImportPackage') }} p
),
package_data AS (
    SELECT *
    FROM {{ source(migration_schema, 'package_key_migrate') }}
),
customer_data AS (
    SELECT
        CustomerAccountNumber,
        CustomerAgentPrefix,
        CustomerAvailableSaturday
    FROM {{ source(migration_schema, 'Customer') }}
),
existing_packages AS (
    {% if is_incremental() %}
    SELECT PackageNumber FROM {{ this }}
    {% else %}
    SELECT NULL as PackageNumber WHERE 1=0  -- Empty result on first run since nothing exists yet
    {% endif %}
),
transformed AS (
    SELECT
        pd.PackageNumber {{ colsql }} AS PackageNumber,
        pd.InternationalTrackingNumber {{ colsql }} AS PackageInternationalTrackingNu,
        COALESCE(pd.prealert_tracking_number, '') AS PackagePrealertTrackingNumber,
        COALESCE(pd.SupplierInvoiceNumber, '') AS PackageSupplierInvoiceNumber,
        pd.PackageDescription {{ colsql }} AS PackageDescription,
        pd.TariffDescription {{ colsql }} AS PackageTariffDescription,
        CASE
            WHEN pd.hazmat {{ colsql }} = 'yes' THEN 1
            ELSE 0
        END AS PackageHazmat,
        CASE
            WHEN pd.Consolidation {{ colsql }} = 'no' THEN 0
            ELSE 1
        END AS PackagePartMultiplePiece,
        pd.TotalPieces AS PackageTotalPieces,
        CASE
            WHEN pd.Consolidation {{ colsql }} = 'no' THEN 0
            ELSE 1
        END AS PackageConsolidation,
        pd.ConsolidationPackageNumber {{ colsql }} AS PackageConsolidationPackageNum,
        pd.length AS PackageLength,
        -- pd.location_id AS PackageLocationId,
        pd.width AS PackageWidth,
        pd.height AS PackageHeight,
        pd.TotalWeight AS PackageTotalWeight,
        pd.shipper {{ colsql }} AS PackageShipper,
        pd.PackageValue AS PackageValue,
        CONVERT(DATETIME,
            CONVERT(VARCHAR(10), pd.DateFirstSeen, 120) + ' ' +
            CONVERT(VARCHAR(8), pd.TimeFirstSeen, 108)) AS PackageFirstSeenDateTime,
        pd.AccountNumber AS CustomerAccountNumber,
        pd.agent_prefix {{ colsql }} AS CustomerAgentPrefix,
        CASE
            WHEN pd.LocationLastSeen {{ colsql }} = 'Castries' THEN 1
            WHEN pd.LocationLastSeen {{ colsql }}  = 'Head Office' THEN 1
            WHEN pd.LocationLastSeen {{ colsql }}  = 'Rodney Bay Office' THEN 2
            WHEN pd.LocationLastSeen {{ colsql }}  = 'RB BOX 10' THEN 2
            WHEN pd.LocationLastSeen {{ colsql }} = 'Vieux Fort Office' THEN 5
            WHEN pd.LocationLastSeen {{ colsql }} = 'Miami' THEN 6
            WHEN pd.LocationLastSeen {{ colsql }} = 'Front Counter' THEN 6
            WHEN pd.LocationLastSeen {{ colsql }} = 'Local Warehouse' THEN 7
            ELSE NULL
        END AS PackageLocationLastSeenId,
        LEFT(pd.notes, 255) {{ colsql }} AS PackageNotes,
        pd.packed_in_shipment AS PackagePackedInShipment,
        CASE
            WHEN pd.PackageStatus {{ colsql }} = 'Delivered' THEN 'Delivered'
            WHEN pd.PackageStatus {{ colsql }} LIKE '%Scheduled for Delivery%' THEN 'ScheduledForDelivery'
            WHEN pd.PackageStatus {{ colsql }} = 'Received At Warehouse' THEN 'ReceivedWareHouse'
            WHEN pd.PackageStatus {{ colsql }} = 'Scanned at Warehouse' THEN 'ScannedWareHouse'
            WHEN pd.PackageStatus {{ colsql }} = 'Detained at Customs' THEN 'Detained'
            WHEN pd.PackageStatus {{ colsql }} = 'Delivery Cancelled' THEN 'DeliveryCancelled'
            WHEN pd.PackageStatus {{ colsql }} LIKE '%Transit%' THEN 'In Transit'
            WHEN pd.PackageStatus {{ colsql }} = 'Returned to Warehouse' THEN 'Returned'
            WHEN pd.PackageStatus {{ colsql }} = 'Out for Delivery' THEN 'OutForDelivery'
            WHEN pd.PackageStatus {{ colsql }} = 'Received from shipper' THEN 'Received'
            WHEN pd.PackageStatus {{ colsql }} = 'Released from Customs' THEN 'Released'
            ELSE 'Received'
        END {{ colsql }} AS PackageActualStatusName,
        SUBSTRING(pd.PackageNumber, 5, 12) {{ colsql }} AS PackageAirwayBillNumber,
        CASE
            WHEN LEFT(pd.PackageNumber, 3) {{ colsql }} = 'OCE' THEN 1
            ELSE 0
        END AS PackageOcean,
        pd.created_at AS PackageCreationDate,
        12 AS UserId,
        1 AS PackageDuration,
        NULL AS ManifestId,
        pd.TotalWeight AS PackageWeight,

        -- == In the next steps, we the
        -- == Packages Location Id, Storage Id and Storage Type
        -- == Location Id means which location is it at
        -- == Storage Id is only relevant if it is on a shelf or in a box or area
        -- == Storage Type allows us to know if it is a shelf,box or area

        -- ---- This step derives the Location Id based on whether it is
        -- ---- Castries:1,Head Office/Front Counter:1, Rodney Bay/RB:2
        -- ---- Vieux Fort/VF:5, Miami:6
        CASE
            WHEN pd.LocationLastSeen {{ colsql }} = 'Castries' THEN 1
            WHEN pd.LocationLastSeen {{ colsql }} = 'Front Counter' THEN 1
            WHEN pd.LocationLastSeen {{ colsql }} = 'Head Office' THEN 1
            WHEN pd.LocationLastSeen {{ colsql }} = 'Rodney Bay Office' THEN 2
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%RB %' THEN 2
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%Vieux Fort%' THEN 5
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%VF %' THEN 5
            WHEN pd.LocationLastSeen {{ colsql }} = 'Miami' THEN 6
            WHEN pd.LocationLastSeen {{ colsql }} = 'Local Warehouse' THEN 7
            ELSE NULL
        END AS PackageLocationId,

        -- ---- This step derives the Storage Id
        -- ---- based on which shelf, box or area number
        -- ---- the actual number is extracted from the number in the
        -- ---- location last seen (e.g. RB AREA 2) would get a
        -- ---- Storage Id of 2
        -- ---- it sets it to NULL if no shelf, box or area is declared
        CASE
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%RB AREA%'
                THEN CAST(SUBSTRING(pd.LocationLastSeen, 8, 3) AS NUMERIC)
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%RB BOX%'
                THEN CAST(SUBSTRING(pd.LocationLastSeen, 7, 3) AS NUMERIC)
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%RB SHELF%'
                THEN CAST(SUBSTRING(pd.LocationLastSeen, 9, 3) AS NUMERIC)
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%VF AREA%'
                THEN CAST(SUBSTRING(pd.LocationLastSeen, 8, 3) AS NUMERIC)
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%VF BOX%'
                THEN CAST(SUBSTRING(pd.LocationLastSeen, 7, 3) AS NUMERIC)
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%VF SHELF%'
                THEN CAST(SUBSTRING(pd.LocationLastSeen, 8, 3) AS NUMERIC)
            ELSE NULL
        END AS PackageLocationLastStorageId,

        -- ---- This checks if  location last seen
        -- ---- is an area, box or shelf,
        -- ---- and sets it appropriately, otherwise NULL
        CASE
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%AREA%' THEN 'AREA'
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%BOX%' THEN 'BOX'
            WHEN pd.LocationLastSeen {{ colsql }} LIKE '%SHELF%' THEN 'SHELF'
            ELSE NULL
        END {{ colsql }} AS PackageLocationLastStorageType,

        NULL AS RepositoryNumber,
        NULL AS RepositoryType,
        '' AS PackageInvoicePDF,
        0 AS PackageMarked,
        '' AS PackageTransactionIdentifier,
        '' AS PackageSpiToken,
        NULL AS ConsigneeId,
        'Saint Lucia' AS DestinationName,
        '' AS PackagePhoto,
        pd.shipper {{ colsql }} AS CourierName,
        pd.shipper {{ colsql }} AS SupplierName,
        CASE
            WHEN c.CustomerAvailableSaturday = 1 THEN 1
            ELSE 0
        END AS AvailableSaturdayFlag
    FROM list_of_existing_package_numbers s
    JOIN package_data pd ON pd.PackageNumber {{ colsql }} = s.PackageNumber
    JOIN customer_data c
        ON pd.AccountNumber = c.CustomerAccountNumber
        AND c.CustomerAgentPrefix {{ colsql }} = 'BSL'
    WHERE NOT EXISTS (
        SELECT 1 FROM existing_packages ep
        WHERE ep.PackageNumber {{ colsql }} = pd.PackageNumber
    )
)

SELECT * FROM transformed

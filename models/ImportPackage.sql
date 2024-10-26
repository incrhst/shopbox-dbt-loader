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
            WHEN pd.hazmat = 'yes' THEN 1
            ELSE 0
        END AS PackageHazmat,
        CASE
            WHEN pd.Consolidation = 'no' THEN 0
            ELSE 1
        END AS PackagePartMultiplePiece,
        pd.TotalPieces AS PackageTotalPieces,
        CASE
            WHEN pd.Consolidation = 'no' THEN 0
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
        pd.AgentPrefix AS CustomerAgentPrefix,
        CASE
            WHEN pd.LocationLastSeen = 'Castries' THEN 1
            WHEN pd.LocationLastSeen = 'Head Office' THEN 1
            WHEN pd.LocationLastSeen = 'Rodney Bay Office' THEN 2
            WHEN pd.LocationLastSeen = 'RB BOX 10' THEN 2
            WHEN pd.LocationLastSeen = 'Vieux Fort Office' THEN 5
            WHEN pd.LocationLastSeen = 'Miami' THEN 6
            WHEN pd.LocationLastSeen = 'Front Counter' THEN 6
            WHEN pd.LocationLastSeen = 'Local Warehouse' THEN 7
            ELSE NULL
        END AS PackageLocationLastSeenId,
        LEFT(pd.notes, 255) {{ colsql }} AS PackageNotes,
        pd.packed_in_shipment AS PackagePackedInShipment,
        CASE
            WHEN pd.PackageStatus = 'Delivered' THEN 'Delivered'
            WHEN pd.PackageStatus LIKE '%Scheduled for Delivery%' THEN 'ScheduledForDelivery'
            WHEN pd.PackageStatus = 'Received At Warehouse' THEN 'ReceivedWareHouse'
            WHEN pd.PackageStatus = 'Scanned at Warehouse' THEN 'ScannedWareHouse'
            WHEN pd.PackageStatus = 'Detained at Customs' THEN 'Detained'
            WHEN pd.PackageStatus = 'Delivery Cancelled' THEN 'DeliveryCancelled'
            WHEN pd.PackageStatus LIKE '%Transit%' THEN 'In Transit'
            WHEN pd.PackageStatus = 'Returned to Warehouse' THEN 'Returned'
            WHEN pd.PackageStatus = 'Out for Delivery' THEN 'OutForDelivery'
            WHEN pd.PackageStatus = 'Received from shipper' THEN 'Received'
            WHEN pd.PackageStatus = 'Released from Customs' THEN 'Released'
            ELSE 'Received'
        END {{ colsql }} AS PackageActualStatusName,
        SUBSTRING(pd.PackageNumber, 5, 12) {{ colsql }} AS PackageAirwayBillNumber,
        CASE
            WHEN LEFT(pd.PackageNumber, 3) = 'OCE' THEN 1
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
            WHEN pd.LocationLastSeen = 'Castries' THEN 1
            WHEN pd.LocationLastSeen = 'Front Counter' THEN 1
            WHEN pd.LocationLastSeen = 'Head Office' THEN 1
            WHEN pd.LocationLastSeen = 'Rodney Bay Office' THEN 2
            WHEN pd.LocationLastSeen LIKE '%RB %' THEN 2
            WHEN pd.LocationLastSeen LIKE '%Vieux Fort%' THEN 5
            WHEN pd.LocationLastSeen LIKE '%VF %' THEN 5
            WHEN pd.LocationLastSeen = 'Miami' THEN 6
            WHEN pd.LocationLastSeen = 'Local Warehouse' THEN 7
            ELSE NULL
        END AS PackageLocationId,

        -- ---- This step derives the Storage Id
        -- ---- based on which shelf, box or area number
        -- ---- the actual number is extracted from the number in the
        -- ---- location last seen (e.g. RB AREA 2) would get a
        -- ---- Storage Id of 2
        -- ---- it sets it to NULL if no shelf, box or area is declared
        CASE
            WHEN pd.LocationLastSeen LIKE '%RB AREA%'
                THEN CAST(SUBSTRING(pd.LocationName, 8, 3) AS NUMERIC)
            WHEN pd.LocationLastSeen LIKE '%RB BOX%'
                THEN CAST(SUBSTRING(pd.LocationName, 7, 3) AS NUMERIC)
            WHEN pd.LocationLastSeen LIKE '%RB SHELF%'
                THEN CAST(SUBSTRING(pd.LocationName, 9, 3) AS NUMERIC)
            WHEN pd.LocationName LIKE '%VF AREA%'
                THEN CAST(SUBSTRING(pd.LocationName, 8, 3) AS NUMERIC)
            WHEN pd.LocationName LIKE '%VF BOX%'
                THEN CAST(SUBSTRING(pd.LocationName, 7, 3) AS NUMERIC)
            WHEN pd.LocationName LIKE '%VF SHELF%'
                THEN CAST(SUBSTRING(pd.LocationName, 8, 3) AS NUMERIC)
            ELSE NULL
        END AS PackageLocationLastStorageId,

        -- ---- This checks if  location last seen
        -- ---- is an area, box or shelf,
        -- ---- and sets it appropriately, otherwise NULL
        CASE
            WHEN pd.LocationLastSeen LIKE '%AREA%' THEN 'AREA'
            WHEN pd.LocationLastSeen LIKE '%BOX%' THEN 'BOX'
            WHEN pd.LocationLastSeen LIKE '%SHELF%' THEN 'SHELF'
            ELSE NULL
        END AS PackageLocationLastStorageType,

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
    JOIN package_data pd ON pd.PackageNumber = s.PackageNumber
    JOIN customer_data c
        ON pd.AccountNumber = c.CustomerAccountNumber
        AND c.CustomerAgentPrefix = 'BSL'
    WHERE NOT EXISTS (
        SELECT 1 FROM existing_packages ep
        WHERE ep.PackageNumber = pd.PackageNumber
    )
)

SELECT * FROM transformed

-- models/PreAlert.sql
{{
    config(
        materialized='table',
         post_hook=[
      "ALTER TABLE {{ this }} ADD PreAlertId INT IDENTITY(1,1) NOT NULL;"
    ]
    )
}}
WITH source AS (
    SELECT *
    FROM {{ source('migration', 'prealert_standalone_migrate') }}
),
transformed AS (
    SELECT
        AccountNumber as CustomerAccountNumber
        CAST(COALESCE(LEFT(AgentPrefix, 3), '') as nvarchar(3)) as CustomerAgentPrefix,
        Value as PreAlertAmount,
        Shipper as PreAlertCourierName,
        -- dates must be correct !!!!!!
        CAST(created_at as datetime) as PreAlertCreatedAt,
        CAST(DateSet as datetime) as PreAlertDateSet,
        CAST(COALESCE(Description, '') as nvarchar(200)) as PreAlertDescription,
        CAST(CASE
            WHEN Displayed = 'yes' THEN 1
            ELSE 0
        END as bit) as PreAlertDisplayed,
        CAST(COALESCE(TRY_CAST(UserId as int), 0) as int) as PreAlertUserId,
        CAST(COALESCE(InternationalTrackingNumber, '') as nvarchar(50)) as PreAlertInternationalTrackingN,
        CAST(NULL as varbinary(max)) as PreAlertInvoicePDF, -- Not in source, setting NULL
        CAST(COALESCE(LastUpdate, GETDATE()) as datetime) as PreAlertLastUpdate,
        CAST(COALESCE(Notes, '') as nvarchar(-1)) as PreAlertNotes, -- nvarchar(-1) means nvarchar(max) in sqlserver
        CAST(0 as bit) as PreAlertSentAMAD, -- Not in source, defaulting to 0
        CAST(COALESCE(NULL, 'N') as nchar(1)) as PreAlertStatus -- Not in source, defaulting to 'N',
        CAST(COALESCE(Supplier, '') as nvarchar(100)) as PreAlertSupplierName,
    FROM source
)
SELECT * FROM transformed

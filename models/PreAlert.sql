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
    FROM {{ source('migration', 'prealert_key_migrate') }}
),
transformed AS (
    SELECT
        CAST(COALESCE(international_tracking_number, '') as nvarchar(50)) as PreAlertInternationalTrackingN,
        CAST(COALESCE(account_number, 0) as int) as CustomerAccountNumber,
        CAST(COALESCE(value, 0) as money) as PreAlertAmount,
        CAST(COALESCE(description, '') as nvarchar(200)) as PreAlertDescription,
        CAST(COALESCE(notes, '') as nvarchar(max)) as PreAlertNotes,
        CAST(CASE 
            WHEN displayed = 'yes' THEN 1
            ELSE 0
        END as bit) as PreAlertDisplayed,
        CAST(COALESCE(date_set, GETDATE()) as datetime) as PreAlertDateSet,
        CAST(COALESCE(last_update, GETDATE()) as datetime) as PreAlertLastUpdate,
        CAST(COALESCE(TRY_CAST(user_id as int), 0) as int) as PreAlertUserId,
        CAST(COALESCE(created_at, GETDATE()) as datetime) as PreAlertCreatedAt,
        CAST(NULL as varbinary(max)) as PreAlertInvoicePDF, -- Not in source, setting NULL
        CAST(COALESCE(LEFT(agent_prefix, 3), '') as nvarchar(3)) as CustomerAgentPrefix,
        CAST(0 as bit) as PreAlertSentAMAD, -- Not in source, defaulting to 0
        CAST(COALESCE(shipper, '') as nvarchar(100)) as PreAlertCourierName,
        CAST(COALESCE(supplier, '') as nvarchar(100)) as PreAlertSupplierName,
        CAST(COALESCE(NULL, 'N') as nchar(1)) as PreAlertStatus -- Not in source, defaulting to 'N'
    FROM source
)
SELECT * FROM transformed

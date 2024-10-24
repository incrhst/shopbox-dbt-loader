-- models/OtherCharge.sql
{{
  config(
    materialized='table',
    post_hook=[
      "ALTER TABLE {{ this }} ADD OtherChargeId DECIMAL(11,0) IDENTITY(1,1) NOT NULL;"
    ]
  )
}}

SELECT
    DateCreated AS OtherChargeCreatedDate,
    TimeCreated AS OtherChargeCreatedTime,
    Amount AS OtherChargeAmount,
    InvoiceNumber AS InvoiceNumber,
    UserId AS OtherChargeUserId,
    PaymentMethod AS PaymentMethodName
FROM {{ source('migration', 'othercharge_migrate') }}

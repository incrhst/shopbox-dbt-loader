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
    date_created AS OtherChargeCreatedDate,
    time_created AS OtherChargeCreatedTime,
    amount AS OtherChargeAmount,
    invoice_number AS InvoiceNumber,
    user_id AS OtherChargeUserId,
    payment_method AS PaymentMethodName
FROM {{ source('migration', 'othercharge_migrate') }}

{{
    config(
        materialized='table',
        post_hook=[
          "ALTER TABLE {{ this }} ADD PaymentId DECIMAL(11,0) IDENTITY(1,1) NOT NULL;"
        ]
    )
}}

WITH source AS (
    SELECT
       payment_date as PaymentDate,
       payment_time as PaymentTime,
       amount as  PaymentAmount,
       invoice_number as  InvoiceNumber,
        payment_time as PaymentCreatedDateTime,
        created_by,  -- Assuming created_by is in the source data
        '' as PaymentComment,
        NULL as LocationId,
        payment_method as PaymentMethodName
    FROM {{ source('migration', 'payment_migrate') }}
),

-- Map created_by to PaymentUserId
transformed AS (
    SELECT
        PaymentDate,
        PaymentTime,
        PaymentAmount,
        InvoiceNumber,
        PaymentCreatedDateTime,
        CASE 
            WHEN created_by = 'bochilien' THEN 12
            WHEN created_by = 'cochilien' THEN 14
            WHEN created_by = 'dochilien' THEN 23
            WHEN created_by = 'aochilien' THEN 28
            WHEN created_by = 'sjan' THEN 40
            WHEN created_by = 'ddaniel' THEN 114
            WHEN created_by = 'aalbert' THEN 119
            WHEN created_by = 'sharris' THEN 126
            WHEN created_by = 'ledward' THEN 128
            WHEN created_by = 'dvalmont' THEN 137
            WHEN created_by = 'sbrown' THEN 139
            WHEN created_by = 'remmanuel' THEN 167
            WHEN created_by = 'rfelicien' THEN 169
            ELSE NULL  -- Handle any unmapped or unexpected values
        END AS PaymentUserId,
        PaymentComment as comment,
        LocationId as location_id,
        PaymentMethodName as payment_method
    FROM source
)

SELECT * FROM transformed;


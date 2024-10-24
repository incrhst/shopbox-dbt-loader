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
       PaymentDate,
       PaymentTime,
       amount as  PaymentAmount,
       InvoiceNumber,
       PaymentTime as PaymentCreatedDateTime,
       Route as source_route,
       CreatedBy,  -- Assuming created_by is in the source data
       LocationId,
       PaymentMethod as PaymentMethodName
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
        CASE
            WHEN source_route = 'Castries Office' THEN 1
            WHEN source_route = 'Rodney Bay Office' THEN 263
            WHEN source_route = 'Hold at Office' THEN 263  -- Assuming 'Hold at Office' means 'Rodney Bay Office'
            WHEN source_route = 'Vieux Fort Office' THEN 316
            WHEN source_route = 'Castries' THEN 253
            WHEN source_route = 'Dennery' THEN 254
            WHEN source_route = 'Micoud' THEN 255
            WHEN source_route = 'Vieux Fort' THEN 256
            WHEN source_route = 'Gros Islet' THEN 252
            WHEN source_route = 'Soufriere' THEN 258
            WHEN source_route = 'Anse La Raye' THEN 260
            WHEN source_route = 'Choiseul' THEN 257
            WHEN source_route = 'Canaries' THEN 259
            ELSE NULL  -- Fallback for unmatched routes
        END AS LocationId,
        PaymentMethod as PaymentMethodName
    FROM source
)

SELECT * FROM transformed;


-- models/staging/InvoiceDetails.sql
{{
    config(
        materialized='incremental',
        unique_key=['InvoiceNumber', 'InvoiceDetailId']
    )
}}
with source as (
    select * from {{ source('migration', 'invoicedetails_migrate') }}
),
transformed as (
    select
        -- Convert bigint to decimal(11,0)
        cast(InvoiceNumber as decimal(11,0)) as InvoiceNumber,
        -- Convert bigint to decimal(11,0)
        cast(id as decimal(11,0)) as InvoiceDetailId,
        -- Convert varchar(50) to nvarchar(100) with proper collation
        cast(charge_description as nvarchar(100)) collate Modern_Spanish_CI_AS as InvoiceDetailChargeName,
        -- Convert numeric to money
        cast(charge_value as money) as InvoiceDetailChargeAmount,
        -- Add missing VAT column with default value
        cast(0.00 as money) as InvoiceDetailChargeVat
    from source
),
final as (
    select
        t.InvoiceNumber,
        t.InvoiceDetailId,
        t.InvoiceDetailChargeName,
        t.InvoiceDetailChargeAmount,
        t.InvoiceDetailChargeVat
    from transformed t
    where not exists (
        select 1
        from {{ source('migration', 'invoicedetails_migrate') }} id
        where cast(id.InvoiceNumber as decimal(11,0)) = t.InvoiceNumber
        and cast(id.id as decimal(11,0)) = t.InvoiceDetailId
    )
)
select * from final

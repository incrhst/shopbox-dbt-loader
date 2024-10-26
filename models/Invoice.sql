{{
    config(
        materialized='incremental',
        unique_key='InvoiceNumber',
        incremental_strategy='merge'
    )
}}

{% set colsql = 'COLLATE Modern_Spanish_CI_AS' %}

-- Source data with row numbers to handle duplicates
with source as (
    select
        s.*,
        row_number() over (
            partition by InvoiceNumber
            order by
                InvoiceDate desc,
                case when InvoiceStatus {{ colsql }} = 'PAID' then 1 else 2 end  -- Prefer PAID status
        ) as row_num
    from {{ source('migration', 'invoicemaster_key_migrate') }} s
)

-- Transform data, applying logic for routes and status
, transformed as (
    select
        s.InvoiceNumber,
        s.InvoiceDate,
        s.PackageNumber,
        s.Consignee {{ colsql }} as InvoiceConsignee,
        s.Shipper {{ colsql }} as InvoiceShipper,
        CASE Route {{ colsql }}
            WHEN 'Castries Office' THEN 1
            WHEN 'Rodney Bay Office' THEN 263
            WHEN 'Hold at Office' THEN 263
            WHEN 'Vieux Fort Office' THEN 316
            WHEN 'Castries' THEN 253
            WHEN 'Dennery' THEN 254
            WHEN 'Micoud' THEN 255
            WHEN 'Vieux Fort' THEN 256
            WHEN 'Gros Islet' THEN 252
            WHEN 'Soufriere' THEN 258
            WHEN 'Anse La Raye' THEN 260
            WHEN 'Choiseul' THEN 257
            WHEN 'Canaries' THEN 259
            ELSE NULL
        END AS RouteId,
        case
            when s.InvoiceStatus {{ colsql }} = 'PAID' then 'Paid'
            else 'Unpaid'
        end {{ colsql }} as InvoiceStatus,
        upper(trim(s.route)) {{ colsql }} as RouteName,
        s.InvoiceWeight,
        s.InvoicePieces,
        case when s.Printed = 1 then 1 else 0 end as InvoicePrinted,
        case when s.emailed = '1' then 1 else 0 end as InvoiceEmailed,
        case when s.Uploaded {{ colsql }} = 'no' then 0 else 1 end as InvoiceUploaded,
        case when s.AllowEmail {{ colsql }} = 'no' then 0 else 1 end as InvoiceAllowEmail,
        case when s.AllowPrint {{ colsql }} = 'no' then 0 else 1 end as InvoiceAllowPrint,
        s.TimePaidOff AS InvoicePaidOffTime,
        s.TimePaidOff AS InvoicePaidOffDate,
        1 as InvoiceUserId
    from source s
    where s.row_num = 1  -- Keep only the first row per InvoiceNumber
)

-- Final selection, excluding duplicates in incremental mode
select
    t.InvoiceNumber,
    t.InvoiceDate,
    t.PackageNumber,
    t.InvoiceConsignee,
    t.InvoiceShipper,
    t.InvoiceStatus,
    t.RouteId,
    t.InvoiceWeight,
    t.InvoicePieces,
    t.InvoicePrinted,
    t.InvoiceEmailed,
    t.InvoiceUploaded,
    t.InvoiceAllowEmail,
    t.InvoiceAllowPrint,
    t.InvoicePaidOffDate,
    t.InvoicePaidOffTime,
    NULL AS UnKnownId,
    t.InvoiceUserId
from transformed t
where t.PackageNumber {{ colsql }} is not null
{% if is_incremental() %}
and not exists (
    select 1
    from {{ this }} existing
    where existing.PackageNumber {{ colsql }} = t.PackageNumber
)
{% endif %}

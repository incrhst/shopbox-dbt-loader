{{
    config(
        materialized='incremental',
        unique_key='InvoiceNumber',
        incremental_strategy='merge'
    )
}}

{% set colsql = 'COLLATE Modern_Spanish_CI_AS' %}

-- First, let's identify any duplicates in the source
with source_duplicates as (
    select
        InvoiceNumber,
        count(*) as duplicate_count
    from {{ source('migration', 'invoicemaster_key_migrate') }}
    group by InvoiceNumber
    having count(*) > 1
),

source as (
    select
        s.*,
        -- Add row number to pick the most recent record for duplicates
        row_number() over (
            partition by InvoiceNumber
            order by
                InvoiceDate desc,
                case when InvoiceStatus = 'PAID' then 1 else 2 end  -- Prefer PAID status
        ) as row_num
    from {{ source('migration', 'invoicemaster_key_migrate') }} s
),

existing_invoices as (
    select InvoiceNumber
    from {{ source('migration', 'Invoice') }}
    {% if is_incremental() %}
    UNION
    select DISTINCT InvoiceNumber
    from {{ this }}
    {% endif %}
),

transformed as (
    select
        s.InvoiceNumber AS InvoiceNumber,
        s.InvoiceDate AS InvoiceDate,
        s.PackageNumber AS PackageNumber,
        s.Consignee {{ colsql }} as InvoiceConsignee,
        s.Shipper {{ colsql }} as InvoiceShipper,
        CASE Route
            WHEN 'Castries Office' THEN 1
            WHEN 'Rodney Bay Office' THEN 263
            WHEN 'Hold at Office' THEN 263  -- Assuming 'Hold at Office' means 'Rodney Bay Office'
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
            ELSE NULL  -- Fallback for unmatched routes
        END AS RouteId,
        case
            when s.InvoiceStatus = 'PAID' then 'Paid'
            else 'Unpaid'
        end {{ colsql }} as InvoiceStatus,
        upper(trim(s.route)) {{ colsql }} as RouteName,
        s.InvoiceWeight AS InvoiceWeight,
        s.InvoicePieces AS InvoicePieces,
        case when s.Printed = 1 then 1 else 0 end as InvoicePrinted,
        case when s.emailed = '1' then 1 else 0 end as InvoiceEmailed,
        case when s.Uploaded = 'no' then 0 else 1 end as InvoiceUploaded,
        case when s.AllowEmail = 'no' then 0 else 1 end as InvoiceAllowEmail,
        case when s.AllowPrint = 'no' then 0 else 1 end as InvoiceAllowPrint,
        s.TimePaidOff AS InvoicePaidOffTime,
        s.TimePaidOff AS InvoicePaidOffDate,
        1 as InvoiceUserId
    from source s
    where s.row_num = 1  -- Take only the first row for each InvoiceNumber
    and not exists (
        select 1
        from existing_invoices e
        where e.InvoiceNumber = s.InvoiceNumber
    )
),

final as (
    select distinct
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
)

-- Add safety check for duplicates before final output
select f.*
from final f
where InvoiceNumber is not null
{% if is_incremental() %}
and not exists (
    select 1
    from {{ this }} existing
    where existing.InvoiceNumber = f.InvoiceNumber
)
{% endif %}

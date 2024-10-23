{{
    config(
        materialized='incremental',
        unique_key='InvoiceNumber'
    )
}}

{% set colsql = 'COLLATE Modern_Spanish_CI_AS' %}

-- First, let's identify any duplicates in the source
with source_duplicates as (
    select 
        invoice_number,
        count(*) as duplicate_count
    from {{ source('migration', 'invoicemaster_key_migrate') }}
    group by invoice_number
    having count(*) > 1
),

source as (
    select 
        s.*,
        -- Add row number to pick the most recent record for duplicates
        row_number() over (
            partition by invoice_number 
            order by 
                invoice_date desc,
                case when invoice_status = 'PAID' then 1 else 2 end  -- Prefer PAID status
        ) as row_num
    from {{ source('migration', 'invoicemaster_key_migrate') }} s
),

existing_invoices as (
    select InvoiceNumber
    from {{ source('migration', 'Invoice') }}
    {% if is_incremental() %}
    UNION
    select InvoiceNumber
    from {{ this }}
    {% endif %}
),

transformed as (
    select
        s.invoice_number AS InvoiceNumber,
        s.invoice_date AS InvoiceDate,
        s.package_number AS PackageNumber,
        s.consignee {{ colsql }} as InvoiceConsignee,
        s.shipper {{ colsql }} as InvoiceShipper,
        case
            when s.invoice_status = 'PAID' then 'PAID'
            else 'UNPAID'
        end {{ colsql }} as InvoiceStatus,
        upper(trim(s.route)) {{ colsql }} as RouteName,
        s.invoice_weight AS InvoiceWeight,
        s.invoice_pieces AS InvoicePieces,
        case when s.printed = 1 then 1 else 0 end as InvoicePrinted,
        case when s.emailed = '1' then 1 else 0 end as InvoiceEmailed,
        case when s.uploaded = 'no' then 0 else 1 end as InvoiceUploaded,
        case when s.allow_email = 'no' then 0 else 1 end as InvoiceAllowEmail,
        case when s.allow_print = 'no' then 0 else 1 end as InvoiceAllowPrint,
        cast('1900-01-01' as date) AS InvoicePaidOffTime,
        cast('1900-01-01' as date) AS InvoicePaidOffDate,
        1 as InvoiceUserId
    from source s
    where s.row_num = 1  -- Take only the first row for each invoice_number
    and not exists (
        select 1
        from existing_invoices e
        where e.InvoiceNumber = s.invoice_number
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
        null as RouteId,
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
     and InvoiceNumber <> 'HAWB076791650000'
{% if is_incremental() %}
and not exists (
    select 1 
    from {{ this }} existing
    where existing.InvoiceNumber = f.InvoiceNumber
)
{% endif %}

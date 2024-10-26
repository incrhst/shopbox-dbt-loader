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
    select InvoiceNumber
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
     and InvoiceNumber <> 'HAWB076791650000'
{% if is_incremental() %}
and not exists (
    select 1 
    from {{ this }} existing
    where existing.InvoiceNumber = f.InvoiceNumber
)
{% endif %}

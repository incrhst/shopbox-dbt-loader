{{
    config(
        materialized='table',
        post_hook=[
            "ALTER TABLE {{ this }} ADD SignUpId INT IDENTITY(1,1) NOT NULL"
        ]
    )
}}

WITH source AS (
    SELECT
        date_logged as SignUpLoggedDate,
        time_logged as SignUpLoggedTime,
        ip_address as SignUpIPAddress,
        id_type_id as IdTypeId,
        first_name as SignUpFirstName,
        last_name as SignUpLastName,
        contact_first_name as SignUpContactFirstName,
        contact_last_name as SignUpContactLastName,
        NULL as SignUpContactIdTypeId,
        email as SignUpEmail,
        company as SignUpCompany,
        tel1 as SignUpMainPhone,
        tel2 as SignUpSecondPhone,
        tel3 as SignUpThirdPhone,
        fax as SignUpFax,
        CONCAT(
            COALESCE(primary__delivery_street1, ''), 
            CASE WHEN primary__delivery_street2 IS NOT NULL THEN ' ' + primary__delivery_street2 ELSE '' END,
            CASE WHEN primary__delivery_city IS NOT NULL THEN ' ' + primary__delivery_city ELSE '' END
        ) as SignUpPrimaryDeliveryAddress,
        insurance_accepted as SignUpInsuranceAccepted,
        1 as SignUpAccountTypeId,
        account_number as SignUpAccountNumber,
        processed as SignUpProcessed,
        reference as SignUpReference,
        CONCAT(
            COALESCE(residential_street1, ''),
            CASE WHEN residential_street2 IS NOT NULL THEN ' ' + residential_street2 ELSE '' END,
            CASE WHEN residential_city IS NOT NULL THEN ' ' + residential_city ELSE '' END
        ) as SignUpResidentialAddress,
        created_at as SignUpCreatedAt,
        updated_at as SignUpUpdatedAt,
        '' as SignUpIDImage,
        '' as SignUpImage,
        NULL as SignUpContactEmail,
        NULL as SignUpSecondContactEmail,
        NULL as SignUpSecondContactFirstName,
        NULL as SignUpSecondContactLastName
    FROM {{ source('migration', 'signups_key_migrate') }}
)

SELECT *
FROM source

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
        DateLogged as SignUpLoggedDate,
        TimeLogged as SignUpLoggedTime,
        IpAddress as SignUpIPAddress,
        IdTypeId as IdTypeId,
        FirstName as SignUpFirstName,
        LastName as SignUpLastName,
        ContactFirstName as SignUpContactFirstName,
        ContactLastName as SignUpContactLastName,
        NULL as SignUpContactIdTypeId,
        Email as SignUpEmail,
        Company as SignUpCompany,
        Tel1 as SignUpMainPhone,
        Tel2 as SignUpSecondPhone,
        Tel3 as SignUpThirdPhone,
        Fax as SignUpFax,
        CONCAT(
            COALESCE(PrimaryDeliveryStreet1, ''),
            CASE WHEN PrimaryDeliveryStreet2 IS NOT NULL THEN ' ' + primarydeliverystreet2 ELSE '' END,
            CASE WHEN PrimaryDeliveryCity IS NOT NULL THEN ' ' + primarydeliverycity ELSE '' END
        ) as SignUpPrimaryDeliveryAddress,
        insuranceaccepted as SignUpInsuranceAccepted,
        1 as SignUpAccountTypeId,
        accountnumber as SignUpAccountNumber,
        processed as SignUpProcessed,
        reference as SignUpReference,
        CONCAT(
            COALESCE(ResidentialStreet1, ''),
            CASE WHEN ResidentialStreet2 IS NOT NULL THEN ' ' + residentialstreet2 ELSE '' END,
            CASE WHEN ResidentialCity IS NOT NULL THEN ' ' + residentialcity ELSE '' END
        ) as SignUpResidentialAddress,
        created_at as SignUpCreatedAt,
        updated_at as SignUpUpdatedAt,
        '' as SignUpIDImage,
        '' as SignUpImage,
        NULL as SignUpContactEmail,
        NULL as SignUpSecondContactEmail,
        NULL as SignUpSecondContactFirstName,
        NULL as SignUpSecondContactLastName
    FROM {{ source('migration', 'signups_standalone_migrate') }}
)

SELECT *
FROM source

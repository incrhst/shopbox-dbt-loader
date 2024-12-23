{{ config(
    materialized='table',
    unique_key=['CustomerAgentPrefix', 'CustomerAccountNumber'],
    full_refresh=true
) }}

{% set colsql = 'COLLATE SQL_Latin1_General_CP1_CI_AS' %} -- Modern_Spanish_CI_AS

WITH source_customers AS (
    SELECT DISTINCT
        AccountNumber AS CustomerAccountNumber,
        AgentPrefix {{ colsql }} AS CustomerAgentPrefix,
        CONCAT(AgentPrefix {{ colsql }}, AccountNumber, ' ', FirstName {{ colsql }}, ' ', LastName {{ colsql }}) AS CustomerName,
        Title {{ colsql }} AS CustomerTitle,
        FirstName {{ colsql }} AS CustomerFirstName,
        '1900-01-01' AS CustomerBirthDate,
        ' ' AS CustomerImageProfile,
        LastName {{ colsql }} AS CustomerLastName,
        DateStarted AS CustomerStartedDate,
        CASE WHEN HasContact {{ colsql }} = 'yes' THEN 1 ELSE 0 END AS CustomerHasContact,
        CustomerPassword {{ colsql }} AS CustomerPassword,
        -- AccountTypeID {{ colsql }} AS AccountTypeId,
        Email {{ colsql }} AS CustomerEmail,
        Company {{ colsql }} AS CompanyName,
        Tel1 {{ colsql }} AS CustomerMainPhone,
        Tel2 {{ colsql }} AS CustomerSecondPhone,
        Tel3 {{ colsql }} AS CustomerThirdPhone,
        Work_tel {{ colsql }} AS CustomerWorkPhone,
        Fax {{ colsql }} AS CustomerFax,
        RouteID AS CustomerDefaultRouteId,
        ResidentialStreet1 {{ colsql }} AS CustomerResidentialStreet1,
        ResidentialStreet2 {{ colsql }} AS CustomerResidentialStreet2,
        ResidentialCity {{ colsql }} AS CustomerResidentialCity,
        Primary_DeliveryStreet1 {{ colsql }} AS CustomerPrimaryDeliveryStreet1,
        Primary_DeliveryStreet2 {{ colsql }} AS CustomerPrimaryDeliveryStreet2,
        Secondary_DeliveryStreet1 {{ colsql }} AS CustomerSecDeliveryStreet1,
        Secondary_DeliveryStreet2 {{ colsql }} AS CustomerSecDeliveryStreet2,
        Secondary_DeliveryCity {{ colsql }} AS CustomerSecondaryDeliveryCity,
        Primary_DeliveryCity {{ colsql }} AS CustomerPrimaryDeliveryCity,
        IDNumber AS CustomerIdNumber,
        IDTypeID AS IdTypeId,
        CASE WHEN InsuranceAccepted {{ colsql }} = 'yes' THEN 1 ELSE 0 END AS CustomerInsuranceAccepted,
        CASE WHEN CreditCardOnFile {{ colsql }} = 'yes' THEN 1 ELSE 0 END AS CustomerCreditCardOnFile,
        CASE WHEN Active {{ colsql }} = 'yes' THEN 1 ELSE 0 END AS CustomerIsActive,
        CASE WHEN AvailableSaturday {{ colsql }} = 'yes' THEN 1 ELSE 0 END AS CustomerAvailableSaturday,
        '' AS CustomerReference
    FROM {{ source('migration', 'customer_standalone_migrate') }}
),

existing_customers AS (
    SELECT
        CustomerAccountNumber,
        CustomerAgentPrefix {{ colsql }} AS CustomerAgentPrefix,
        '' AS CustomerReference
    FROM {{ source('migration', 'Customer') }}
),

missing_customers AS (
    SELECT
        sc.CustomerAccountNumber,
        sc.CustomerBirthDate,
        sc.CustomerImageProfile,
        sc.CustomerAgentPrefix,
        sc.CustomerName,
        sc.CustomerTitle,
        sc.CustomerFirstName,
        sc.CustomerLastName,
        sc.CustomerStartedDate,
        sc.CustomerHasContact,
        sc.CustomerEmail,
        sc.CustomerMainPhone,
        sc.CustomerSecondPhone,
        sc.CustomerThirdPhone,
        sc.CustomerWorkPhone,
        sc.CustomerFax,
        sc.CustomerDefaultRouteId,
        sc.CompanyName,
        sc.CustomerIdNumber,
        sc.IdTypeId,
        sc.CustomerInsuranceAccepted,
        sc.CustomerCreditCardOnFile,
        sc.CustomerIsActive,
        sc.CustomerAvailableSaturday,
        sc.CustomerResidentialStreet1,
        sc.CustomerResidentialStreet2,
        sc.CustomerResidentialCity,
        sc.CustomerPrimaryDeliveryStreet1,
        sc.CustomerPrimaryDeliveryStreet2,
        sc.CustomerPrimaryDeliveryCity,
        sc.CustomerSecDeliveryStreet1,
        sc.CustomerSecDeliveryStreet2,
        sc.CustomerSecondaryDeliveryCity,
        sc.CustomerReference
    FROM source_customers sc
    LEFT JOIN existing_customers ec
        ON sc.CustomerAccountNumber = ec.CustomerAccountNumber
        AND sc.CustomerAgentPrefix {{ colsql }} = ec.CustomerAgentPrefix
    WHERE ec.CustomerAccountNumber IS NULL
    AND ec.CustomerAgentPrefix IS NULL
)

SELECT
    mc.CustomerAccountNumber,
    mc.CustomerBirthDate,
    mc.CustomerImageProfile,
    mc.CustomerAgentPrefix,
    mc.CustomerName,
    mc.CustomerTitle,
    mc.CustomerFirstName,
    mc.CustomerLastName,
    mc.CustomerStartedDate,
    mc.CustomerHasContact,
    mc.CustomerEmail,
    mc.CustomerMainPhone,
    mc.CustomerSecondPhone,
    mc.CustomerThirdPhone,
    mc.CustomerWorkPhone,
    mc.CustomerFax,
    mc.CustomerIsActive,
    mc.CustomerAvailableSaturday,
    mc.CustomerDefaultRouteId,
    mc.CompanyName,
    mc.CustomerIdNumber,
    mc.CustomerPassword,
    mc.AccountTypeId,
    mc.IdTypeId,
    mc.CustomerInsuranceAccepted,
    mc.CustomerCreditCardOnFile,
    mc.CustomerResidentialStreet1,
    mc.CustomerResidentialStreet2,
    mc.CustomerResidentialCity,
    mc.CustomerPrimaryDeliveryStreet1,
    mc.CustomerPrimaryDeliveryStreet2,
    mc.CustomerPrimaryDeliveryCity,
    mc.CustomerSecDeliveryStreet1,
    mc.CustomerSecDeliveryStreet2,
    mc.CustomerSecondaryDeliveryCity,
    mc.CustomerReference,
    758 AS CustomerCountryId
FROM missing_customers mc

{% if is_incremental() %}
WHERE mc.CustomerAccountNumber NOT IN (SELECT CustomerAccountNumber FROM {{ this }})
{% endif %}

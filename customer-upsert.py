import os
from sqlalchemy import create_engine, Table, MetaData, select, insert
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the database URLs from the environment
source_db_url = os.getenv("SOURCE_DB_URL")
target_db_url = os.getenv("TARGET_DB_URL")

# Connect to the source and target databases
source_engine = create_engine(source_db_url)
target_engine = create_engine(target_db_url)

# Define table metadata
metadata = MetaData()

# Define source and target tables (assuming similar schemas in source and target)
source_customers = Table("source_customers", metadata, autoload_with=source_engine)
target_customers = Table("target_customers", metadata, autoload_with=target_engine)

# Perform the upsert operation
with Session(target_engine) as target_session:
    with source_engine.connect() as source_conn:
        # Fetch source customer records
        source_query = select(source_customers)
        source_results = source_conn.execute(source_query).fetchall()

        for row in source_results:
            # Prepare the upsert logic
            upsert_stmt = insert(target_customers).values(
                CustomerAccountNumber=row['CustomerAccountNumber'],
                CustomerAgentPrefix=row['CustomerAgentPrefix'],
                CustomerName=row['CustomerName'],
                CustomerTitle=row['CustomerTitle'],
                CustomerFirstName=row['CustomerFirstName'],
                CustomerBirthDate=row['CustomerBirthDate'],
                CustomerImageProfile=row['CustomerImageProfile'],
                CustomerLastName=row['CustomerLastName'],
                CustomerStartedDate=row['CustomerStartedDate'],
                CustomerHasContact=row['CustomerHasContact'],
                CustomerPassword=row['CustomerPassword'],
                CustomerEmail=row['CustomerEmail'],
                CompanyName=row['CompanyName'],
                CustomerMainPhone=row['CustomerMainPhone'],
                CustomerSecondPhone=row['CustomerSecondPhone'],
                CustomerThirdPhone=row['CustomerThirdPhone'],
                CustomerWorkPhone=row['CustomerWorkPhone'],
                CustomerFax=row['CustomerFax'],
                CustomerDefaultRouteId=row['CustomerDefaultRouteId'],
                CustomerResidentialStreet1=row['CustomerResidentialStreet1'],
                CustomerResidentialStreet2=row['CustomerResidentialStreet2'],
                CustomerResidentialCity=row['CustomerResidentialCity'],
                CustomerPrimaryDeliveryStreet1=row['CustomerPrimaryDeliveryStreet1'],
                CustomerPrimaryDeliveryStreet2=row['CustomerPrimaryDeliveryStreet2'],
                CustomerPrimaryDeliveryCity=row['CustomerPrimaryDeliveryCity'],
                CustomerSecDeliveryStreet1=row['CustomerSecDeliveryStreet1'],
                CustomerSecDeliveryStreet2=row['CustomerSecDeliveryStreet2'],
                CustomerSecondaryDeliveryCity=row['CustomerSecondaryDeliveryCity'],
                CustomerIdNumber=row['CustomerIdNumber'],
                IdTypeId=row['IdTypeId'],
                CustomerInsuranceAccepted=row['CustomerInsuranceAccepted'],
                CustomerCreditCardOnFile=row['CustomerCreditCardOnFile'],
                CustomerIsActive=row['CustomerIsActive'],
                CustomerAvailableSaturday=row['CustomerAvailableSaturday'],
                CustomerReference=row['CustomerReference']
            ).on_conflict_do_update(
                index_elements=['CustomerAccountNumber', 'CustomerAgentPrefix'],
                set_={
                    "CustomerName": row['CustomerName'],
                    "CustomerTitle": row['CustomerTitle'],
                    "CustomerFirstName": row['CustomerFirstName'],
                    "CustomerLastName": row['CustomerLastName'],
                    "CustomerStartedDate": row['CustomerStartedDate'],
                    "CustomerHasContact": row['CustomerHasContact'],
                    "CustomerEmail": row['CustomerEmail'],
                    "CustomerMainPhone": row['CustomerMainPhone'],
                    "CustomerSecondPhone": row['CustomerSecondPhone'],
                    "CustomerThirdPhone": row['CustomerThirdPhone'],
                    "CustomerWorkPhone": row['CustomerWorkPhone'],
                    "CustomerFax": row['CustomerFax'],
                    "CustomerDefaultRouteId": row['CustomerDefaultRouteId'],
                    "CompanyName": row['CompanyName'],
                    "CustomerResidentialStreet1": row['CustomerResidentialStreet1'],
                    "CustomerResidentialStreet2": row['CustomerResidentialStreet2'],
                    "CustomerResidentialCity": row['CustomerResidentialCity'],
                    "CustomerPrimaryDeliveryStreet1": row['CustomerPrimaryDeliveryStreet1'],
                    "CustomerPrimaryDeliveryStreet2": row['CustomerPrimaryDeliveryStreet2'],
                    "CustomerPrimaryDeliveryCity": row['CustomerPrimaryDeliveryCity'],
                    "CustomerSecDeliveryStreet1": row['CustomerSecDeliveryStreet1'],
                    "CustomerSecDeliveryStreet2": row['CustomerSecDeliveryStreet2'],
                    "CustomerSecondaryDeliveryCity": row['CustomerSecondaryDeliveryCity'],
                    "CustomerIdNumber": row['CustomerIdNumber'],
                    "IdTypeId": row['IdTypeId'],
                    "CustomerInsuranceAccepted": row['CustomerInsuranceAccepted'],
                    "CustomerCreditCardOnFile": row['CustomerCreditCardOnFile'],
                    "CustomerIsActive": row['CustomerIsActive'],
                    "CustomerAvailableSaturday": row['CustomerAvailableSaturday'],
                    "CustomerReference": row['CustomerReference']
                }
            )
            # Execute the upsert statement
            target_session.execute(upsert_stmt)

        # Commit the transaction after all upserts
        target_session.commit()

print("Upsert operation completed successfully.")

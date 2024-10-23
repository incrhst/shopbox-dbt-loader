import json
import jsonlines
import pyodbc
import logging
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)

# Connection to SQL Server
connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=your_server;'
    'DATABASE=your_database;'
    'UID=your_username;'
    'PWD=your_password'
)
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

def get_location_number(location: str) -> int:
    """Function to map location names to their IDs."""
    location_mapping = {
        'Castries': 1,
        'Head Office': 1,  
        'Rodney Bay Office': 2,
        'RB BOX 10': 2,  
        'Vieux Fort Office': 5,
        'Miami': 6,
        'Front Counter': 6,  
        'Local Warehouse': 7
    }
    return location_mapping.get(location, None)

def transform_package_data(package: Dict) -> Dict:
    """Transform the package data from JSON before inserting into SQL Server."""
    transformed = {
        "PackageNumber": package['PackageNumber'],
        "InternationalTrackingNumber": package['InternationalTrackingNumber'],
        "PrealertTrackingNumber": " ",  
        "SupplierInvoiceNumber": package['SupplierInvoiceNumber'],
        "PackageDescription": package['PackageDescription'],
        "TariffDescription": package['TariffDescription'],
        "Hazmat": True if package['Hazmat'] == 'Yes' else False,
        "TotalPieces": package['TotalPieces'],
        "Consolidation": True if package['Consolidation'] == 'Yes' else False,
        "ConsolidationPackageNumber": package['ConsolidationPackageNumber'],
        "Length": package['Length'],
        "Width": package['Width'],
        "Height": package['Height'],
        "TotalWeight": package['TotalWeight'],
        "Shipper": package['Shipper'].upper().strip() if package['Shipper'] else "",
        "PackageValue": package['PackageValue'],
        "UserId": 12,  
        "PackageFirstSeenDateTime": package['PackageFirstSeenDateTime'],
        "CustomerAccountNumber": package['AccountNumber'],
        "PackageLocationLastSeenId": get_location_number(package['LocationLastSeen']),
        "Notes": package['Notes'][:255],  
        "PackedInShipment": "",  
        "PackageStatus": package['PackageStatus'],
        "CourierName": None,
        "SupplierName": package['Supplier'],
        "PackageCreationDate": package['created_at'],
        "ManifestId": 1,  
        # Add additional transformed fields as needed
    }
    return transformed

def migrate_packages(file_path: str):
    """Migrate package data from the specified JSONL file."""
    batch_values = []  # To hold batch insert values
    with jsonlines.open(file_path) as reader:
        for package in reader:
            try:
                # Transform the package data
                transformed_data = transform_package_data(package)

                # Prepare the data for batch insert
                package_data = (
                    transformed_data['PackageNumber'],
                    transformed_data['InternationalTrackingNumber'],
                    transformed_data['PrealertTrackingNumber'],
                    transformed_data['SupplierInvoiceNumber'],
                    transformed_data['PackageDescription'],
                    transformed_data['TariffDescription'],
                    transformed_data['Hazmat'],
                    transformed_data['TotalPieces'],
                    transformed_data['Consolidation'],
                    transformed_data['ConsolidationPackageNumber'],
                    transformed_data['Length'],
                    transformed_data['Width'],
                    transformed_data['Height'],
                    transformed_data['TotalWeight'],
                    transformed_data['Shipper'],
                    transformed_data['PackageValue'],
                    transformed_data['UserId'],
                    transformed_data['PackageFirstSeenDateTime'],
                    transformed_data['CustomerAccountNumber'],
                    transformed_data['PackageLocationLastSeenId'],
                    transformed_data['Notes'],
                    transformed_data['PackedInShipment'],
                    transformed_data['PackageStatus'],
                    transformed_data['CourierName'],
                    transformed_data['SupplierName'],
                    transformed_data['PackageCreationDate'],
                    transformed_data['ManifestId'],
                    # Add other fields here
                )

                batch_values.append(package_data)

            except Exception as e:
                logging.error(f'Error transforming package {package.get("PackageNumber")}: {e}')

    # Insert batch values into SQL Server
    if batch_values:
        try:
            cursor.executemany(
                "INSERT INTO Package (PackageNumber, InternationalTrackingNumber, PrealertTrackingNumber, SupplierInvoiceNumber, "
                "PackageDescription, TariffDescription, Hazmat, TotalPieces, Consolidation, ConsolidationPackageNumber, "
                "Length, Width, Height, TotalWeight, Shipper, PackageValue, UserId, PackageFirstSeenDateTime, "
                "CustomerAccountNumber, PackageLocationLastSeenId, Notes, PackedInShipment, PackageStatus, "
                "CourierName, SupplierName, PackageCreationDate, ManifestId) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch_values
            )
            conn.commit()
            logging.info(f'Successfully migrated {len(batch_values)} packages.')

        except Exception as e:
            logging.error(f'Error inserting packages: {e}')

def migrate_other_files(file_path: str):
    """Migrate from other JSONL files."""
    # Implement similar logic as in migrate_packages
    pass

# Main migration process
def main():
    # Migrate packages
    migrate_packages('shopbox-importpackage.jsonl')

    # Migrate other JSONL files
    #migrate_other_files('shopbox-importpackageevent.jsonl')
    #migrate_other_files('shopbox-invoicecredit.jsonl')
    #migrate_other_files('shopbox-invoicedetails.jsonl')

    # Close the database connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()


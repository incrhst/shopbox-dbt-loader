import pyodbc

server = '192.168.11.12'
database = 'LogiksysExtract'
username = 'gx'
password = 'Olympics22'

connection_string = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    f'encrypt=yes;'
    f'TrustServerCertificate=yes;'
)

try:
    with pyodbc.connect(connection_string) as conn:
        print("Connection successful!")
except Exception as e:
    print("Connection failed:", e)


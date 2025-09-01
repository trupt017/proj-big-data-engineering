# upload_to_adls.py
import pandas as pd
from sqlalchemy import create_engine, URL
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# 1. Load environment variables
load_dotenv()
azure_conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
sql_password = os.getenv("LOCAL_SQL_SERVER_PASSWORD")
sql_username = os.getenv("LOCAL_SQL_SERVER_USERNAME")
sql_hostname = os.getenv("LOCAL_SQL_SERVER_HOST")


# 2. Define connections using SQLAlchemy's URL method
try:
    connection_url = URL.create(
        "mssql+pyodbc",
        username=sql_username,
        password=sql_password,
        host=sql_hostname,
        database="dev_FMCG_db",
        query={
            "driver": "ODBC Driver 17 for SQL Server",
            "Trusted_Connection": "no",
            "Connection Timeout": "30"
        }
    )
    local_engine = create_engine(connection_url)
    print("SQL Server engine created successfully.")
except Exception as e:
    print(f"Error creating SQL Server engine: {e}")
    exit(1)

# Connection to AZURE BLOB STORAGE
try:
    blob_service_client = BlobServiceClient.from_connection_string(azure_conn_string)
    container_name = "raw"
    print("Azure Blob Service client created successfully.")
except Exception as e:
    print(f"Error creating Azure client: {e}")
    exit(1)

# 3. Define function to upload a table
def upload_table_to_adls(table_name):
    print(f"Uploading {table_name}...")
    
    try:
        # Read table from local SQL
        df = pd.read_sql_table(table_name, local_engine)
        print(f"Successfully read {len(df)} rows from {table_name}")
        
        # Save DataFrame to a local Parquet file temporarily
        local_parquet_path = f"{table_name}.parquet"
        df.to_parquet(local_parquet_path, index=False)
        
        # Create a blob client and upload the file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{table_name}.parquet")
        with open(local_parquet_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        print(f"Successfully uploaded {table_name}.parquet to Azure Blob Storage.")
        
        # Clean up the local file
        os.remove(local_parquet_path)
        
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# 4. List of tables to upload
tables_to_upload = ["store_info_india", "product_hierarchy_info", "sales_fact"] 

# 5. Run the upload for each table
for table in tables_to_upload:
    upload_table_to_adls(table)

print("All done! Data is in the cloud.")
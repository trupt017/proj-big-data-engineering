# upload_to_adls.py
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

# 1. Load environment variables (your connection string)
load_dotenv()
azure_conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
sql_password = os.getenv("LOCAL_SQL_SERVER_PASSWORD")

# 2. Define connections
# Connection to LOCAL SQL Server (Docker)
local_conn_string = f"mssql+pyodbc://sa:{sql_password}@172.17.0.2/dev_FMCG_db?driver=ODBC+Driver+17+for+SQL+Server"
local_engine = create_engine(local_conn_string)

# Connection to AZURE BLOB STORAGE
blob_service_client = BlobServiceClient.from_connection_string(azure_conn_string)
container_name = "raw"

# 3. Define function to upload a table
def upload_table_to_adls(table_name):
    print(f"Uploading {table_name}...")
    
    # Read table from local SQL
    df = pd.read_sql_table(table_name, local_engine)
    
    # Save DataFrame to a local Parquet file temporarily
    local_parquet_path = f"{table_name}.parquet"
    df.to_parquet(local_parquet_path, index=False)
    
    # Create a blob client and upload the file
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{table_name}.parquet")
    with open(local_parquet_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"Successfully uploaded {table_name}.parquet to Azure Blob Storage.")
    # Optional: Clean up the local file
    os.remove(local_parquet_path)

# 4. List of tables to upload
tables_to_upload = ["store_info_india", "product_hierarchy_info", "sales_fact"] 

# 5. Run the upload for each table
for table in tables_to_upload:
    upload_table_to_adls(table)

print("All done! Data is in the cloud.")
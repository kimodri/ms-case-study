# use the blob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os, sys, json
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

# Get the file to load
if len(sys.argv) < 2:
    print("load.py requires the JSON file to load as an argument")
    sys.exit(1)
elif len(sys.argv) > 2:
    print("load.py accepts only one argument.")
else:
    blob_name = sys.argv[1]

# load the environment variable
load_dotenv()
string_connection = os.getenv("DATABASE_URI")
azure_blob_str_connection = os.getenv("AZURE_BLOB_CON")

# Connect to blob
blob_service_client = BlobServiceClient.from_connection_string(azure_blob_str_connection)
container_name = "files"
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Download the data
try:
    downloaded_data = blob_client.download_blob().readall()
    data = json.loads(downloaded_data)
except ResourceNotFoundError:
    print("Blob does not exist.")
    sys.exit(1)
except Exception as e:
    print("Other error:", e)
    sys.exit(1)

source_name = blob_client.blob_name

print(source_name)
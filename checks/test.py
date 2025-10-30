from azure.storage.blob import BlobServiceClient
import requests, datetime, json, os, io
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
azure_blob_str_connection = os.getenv("AZURE_BLOB_CON")

# connect to blob
blob_service_client = BlobServiceClient.from_connection_string(azure_blob_str_connection)

# get the container
container_name = "files"

file_path = Path("./files/microsoft_learn_catalog_20251030.json")
blob_name = file_path.name  

# Get a blob client
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Upload the file
with open(file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print(f"Uploaded {blob_name} to {container_name}.")

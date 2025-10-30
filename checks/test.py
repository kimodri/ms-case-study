import os
import json
import datetime
import requests
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables
load_dotenv()
azure_blob_str_connection = os.getenv("AZURE_BLOB_CON")

# Initialize Blob service client
try:
    # logger.info("Connecting to Azure storage ...")
    blob_service_client = BlobServiceClient.from_connection_string(azure_blob_str_connection)
    container_name = "files"
    blob_name = f"microsoft_learn_catalog_{datetime.datetime.now():%Y%m%d}.json"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
except Exception as e:
    print("Cannot connect to cloud. Please check connection ...")

# Fetch Microsoft Learn catalog
# url = "https://learn.microsoft.com/api/catalog"
url = "https://api.github.com/repos/python/cpython"
params = {"locale": "en-us"}

try:
    # logger.info("Connecting to Azure storage ...")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    # 1. Serialize the Python dictionary to a JSON string
    json_string = json.dumps(data, indent=4) 
    
    # 2. Convert the string to a stream (or bytes) for uploading
    upload_data = json_string.encode('utf-8')

    # 3. Upload the data to Azure Blob Storage
    # logger.info(f"Uploading to Azure Blob Storage: {container_name}/{blob_name}...")
    blob_client.upload_blob(upload_data, overwrite=True)
    print("Upload complete!")

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
except Exception as e:
    print(f"An error occurred during upload: {e}")

except Exception as e:
    print(f"Cannot upload: {e}")
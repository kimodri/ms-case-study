import os
import json
import datetime
import requests
from utility import logger
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.pipeline.transport import RequestsTransport 

# Load environment variables
load_dotenv()
azure_blob_str_connection = os.getenv("AZURE_BLOB_CON")

# --- GLOBAL CONFIGURATION FOR AZURE TIMEOUT ---
# Set a generous timeout (e.g., 5 minutes or 300 seconds) for the network operation.
LONG_TRANSPORT_TIMEOUT = 300

def fetch_and_upload():
    # Initialize Blob service client
    try:
        logger.info("Connecting to Azure storage ...")
        
        # 1. Create a custom Transport object with the desired timeout
        custom_transport = RequestsTransport(connection_timeout=LONG_TRANSPORT_TIMEOUT) 

        # 2. Pass the custom transport to the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(
            azure_blob_str_connection,
            transport=custom_transport # FIX to the latency issue
        )
        
        container_name = "files"
        
        blob_name = f"microsoft_learn_catalog_{datetime.datetime.now():%Y%m%d}.json" 
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    except Exception as e:
        logger.error("Cannot connect to cloud. Please check connection ...")
        print(f"Error during Azure connection initialization: {e}")
        return

    # Fetch Microsoft Learn catalog
    url = "https://learn.microsoft.com/api/catalog"
    params = {"locale": "en-us"}

    try:
        logger.info("Fetching data from MS Learn API ...")
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        # Serialize the Python dictionary to a JSON string
        json_string = json.dumps(data, indent=4) 
        
        # Convert the string to a stream (or bytes) for uploading
        upload_data = json_string.encode('utf-8')

        # Upload the data to Azure Blob Storage
        logger.info(f"Uploading {len(upload_data) / 1024**2:.2f} MB to Azure Blob Storage: {container_name}/{blob_name}...")
        
        # The transport timeout configured above applies here!
        blob_client.upload_blob(upload_data, overwrite=True) 
        
        print("Upload complete!")

    except requests.exceptions.RequestException as e:
        # Handles errors during the fetch operation
        print(f"Error fetching data: {e}")
    except Exception as e:
        # This will now capture the extended timeout error or other general errors
        print(f"An error occurred during upload: {e}")

if __name__ == "__main__":
    fetch_and_upload()
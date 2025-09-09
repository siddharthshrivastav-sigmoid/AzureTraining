from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# -------------------------
# Step 1: Define parameters
# -------------------------
#connect_str = "DefaultEndpointsProtocol=https;AccountName=siddharthstoragecli;AccountKey=nzErchyjBxU0vuyNikjDba+el/00LfktjdFYG9NBzBZG3ibRjlbuq/RXgKDsB/ydaoVK+fiIyfas+AStLEzbOg==;EndpointSuffix=core.windows.net"container_name = "images"  # Replace with your container name
local_file_path = "./map/download.jpeg"   # Path to the local file to upload
blob_name = os.path.basename(local_file_path)  # Name for the blob in Azure
#print(blob_name)
# -------------------------
# Step 2: Create BlobServiceClient
# -------------------------
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# -------------------------
# Step 3: Create container if it doesn't exist
# -------------------------
container_client = blob_service_client.get_container_client(container_name)
try:
    container_client.create_container()
    print(f"Container '{container_name}' created.")
except Exception as e:
    print(f"Container '{container_name}' already exists or error: {e}")

# -------------------------
# Step 4: Upload file
# -------------------------
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

with open(local_file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)
    print(f"File '{local_file_path}' uploaded to blob '{blob_name}' successfully.")

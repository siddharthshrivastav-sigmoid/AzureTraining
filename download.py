from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO

# -------------------------
# Step 1: Define parameters
# -------------------------

container_name = "images"
blob_name = "top_customers.csv"  # the file you uploaded

# -------------------------
# Step 2: Create BlobServiceClient
# -------------------------
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# -------------------------
# Step 3: Get a BlobClient for the specific blob
# -------------------------
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# -------------------------
# Step 4: Download the blob
# -------------------------
download_stream = blob_client.download_blob()
file_content = download_stream.readall().decode("utf-8")

print("File content from Blob Storage:")
print(file_content)

# -------------------------
# Step 5 (Optional): If it's a CSV, load into Pandas
# -------------------------
df = pd.read_csv(StringIO(file_content))
print("\nDataFrame preview:")
print(df.head())

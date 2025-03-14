# Original script by Ronald Cheung
# Out of the many in the zoomcamp knowledge repositiories this is the best one out there!!
import requests
from google.cloud import storage
import os
import logging

# Set up logging to log to a file and console
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("url2bucket.log"),
                        logging.StreamHandler()
                    ])

# Get credentials path from the environment variable
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_FOR_dbt")

#Change this to your bucket name
BUCKET_NAME = "module-4-bucket"  

# Initialize storage client with explicit credentials
storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
print("Storage client initialized with dbt credentials from credentials file")

# Get your bucket
bucket = storage_client.get_bucket(BUCKET_NAME)
print(f"Connection to Bucket {BUCKET_NAME} initialized.")

# Define the base URLs for the data
base_urls = {
    'yellow': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_',
    'green': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_',
    'fhv': 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_'
}
# Define the years and months to download
years = ['2019', '2020']
months = [str(month).zfill(2) for month in range(1, 13)]

# Download the data and upload to GCS
for taxi_type, base_url in base_urls.items():
    print(f"Processing taxi type: {taxi_type}")
    for year in years:
        for month in months:
            if taxi_type == 'fhv' and year == '2020':
                print(f"Skipping FHV data for year {year}")
                continue
            url = f"{base_url}{year}-{month}.csv.gz"
            try:
                print(f"Fetching data from: {url}")
                response = requests.get(url, timeout=500)
                response.raise_for_status()  # Will raise an exception for bad status codes
                blob = bucket.blob(f"{taxi_type}_tripdata_{year}-{month}.csv.gz")
                blob.upload_from_string(response.content)
                print(f"Successfully uploaded {taxi_type}_tripdata_{year}-{month}.csv.gz to GCS")
            except requests.RequestException as e:
               logging.error(f"Error downloading {url}: {e}")
            except Exception as e:
               logging.error(f"An error occurred while processing {url}: {e}")
import io
import os
import requests
import pandas as pd
from google.cloud import storage
import logging

# Set up logging to log to a file and console
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("web_to_gcs.log"),
                        logging.StreamHandler()
                    ])
# Get credentials path from the environment variable
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_FOR_dbt")

#Change this to your bucket name
BUCKET_NAME = "module-4-bucket"  

services = ['green','yellow', 'fhv']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

# Initialize the client once
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)  # Initialize bucket once here


def upload_to_gcs(bucket, object_name, local_file):
    try:
        blob = bucket.blob(object_name)  # Reuse the bucket object
        blob.upload_from_filename(local_file)
        logging.info(f"Uploaded {local_file} to GCS as {object_name}")
    except Exception as e:
        logging.error(f"Failed to upload {local_file} to GCS: {e}")


def web_to_gcs(year, service):
    for i in range(12):
        month = f"{i+1:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        logging.info(f"Now processing file: {file_name}")

        request_url = f"{init_url}{service}/{file_name}"
        logging.info(f"Downloading from: {request_url}")

        r = requests.get(request_url)
        if r.status_code != 200:
            logging.error(f"Failed to download {file_name} - Status Code: {r.status_code}")
            continue

         # Download file locally
        with open(file_name, 'wb') as f:
            f.write(r.content)
        logging.info(f"Downloaded {file_name} locally")

        try:
            # Read CSV and convert to Parquet
            df = pd.read_csv(file_name, compression='gzip',  low_memory=False)
            parquet_file_name = file_name.replace('.csv.gz', '.parquet')
            df.to_parquet(parquet_file_name, engine='pyarrow')
            logging.info(f"Converted {file_name} to Parquet format: {parquet_file_name}")

            # Try uploading the converted Parquet file to GCS
            try:
                upload_to_gcs(bucket, f"{service}/{parquet_file_name}", parquet_file_name)
                logging.info(f"Uploaded {parquet_file_name} to GCS")
                
                # Clean up the local file after a successful upload
                os.remove(file_name)  # Remove the original .csv.gz file
                os.remove(parquet_file_name)  # Remove the .parquet file
                logging.info(f"Deleted local files: {file_name} and {parquet_file_name}")
            except Exception as e:
                logging.error(f"Failed to upload {parquet_file_name} to GCS: {e}")
                raise Exception("Upload failed, stopping further processing.")

        except Exception as e:
            logging.error(f"Failed to process {file_name}: {e}")
            continue


for service in services:
    # Only process 2019 for 'fhv'
    if service == 'fhv':
        web_to_gcs('2019', service)
    else:
        # Process both 2019 and 2020 for other services
        web_to_gcs('2019', service)
        web_to_gcs('2020', service)


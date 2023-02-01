import os

from google.cloud import storage

def create_bucket(bucket_name,location):
    """Creates a new bucket."""

    storage_client = storage.Client()
    
    bucket = storage_client.create_bucket(bucket_name,location=location)

    print(f"Bucket {bucket.name} created")


storage_client = storage.Client()
buckets = storage_client.list_buckets()

import os
bucket_project = os.environ['GOOGLE_CLOUD_PROJECT']
BUCKET = bucket_project+'_dataobservabilityworkshop'


if BUCKET not in [b.name for b in buckets]:
    create_bucket(BUCKET,location="eu")

def upload_to_gcs_via_pandas(src_file, dst_uri):
    import pandas as pd
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format")
        return
    try: 
        df = pd.read_csv(src_file, parse_dates=['lpep_dropoff_datetime'], infer_datetime_format=True)
    except:
        df = pd.read_csv(src_file, parse_dates=['tpep_dropoff_datetime'], infer_datetime_format=True)
    df.to_csv(dst_uri,index=False)


dataset_url="../../data/green_ingestion.csv"
dataset_file = "green_ingestion.csv"

gs_file_uri = f"gs://{BUCKET}/{dataset_file}"

upload_to_gcs_via_pandas(dataset_url, gs_file_uri)


dataset_url="../../data/yellow_ingestion.csv"
dataset_file = "yellow_ingestion.csv"

gs_file_uri = f"gs://{BUCKET}/{dataset_file}"

upload_to_gcs_via_pandas(dataset_url, gs_file_uri)







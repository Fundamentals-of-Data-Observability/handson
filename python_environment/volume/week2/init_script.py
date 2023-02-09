import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='dbt/google/google_credentials.json'


from google.cloud import storage

def create_bucket(bucket_name,location):
    """Creates a new bucket."""

    storage_client = storage.Client()

    print(f"Creating bucket: {bucket_project}")
    
    bucket = storage_client.create_bucket(bucket_name,location=location)

    print(f"Bucket {bucket.name} created")

import os
bucket_project = os.environ['GOOGLE_CLOUD_PROJECT']


storage_client = storage.Client()
buckets = storage_client.list_buckets()

BUCKET = bucket_project+'_dataobservabilityworkshop'

print(bucket_project)

if BUCKET not in [b.name for b in buckets]:
    create_bucket(BUCKET,location="eu")


from google.cloud import bigquery
import pandas as pd

bq = bigquery.Client()
bq.create_dataset('dataobservability',exists_ok=True)

import os
project=os.getenv('GOOGLE_CLOUD_PROJECT')

data_set_green = pd.read_csv('data/green_ingestion.csv',parse_dates=['lpep_pickup_datetime','lpep_dropoff_datetime'])
data_set_green.to_csv(f"gs://{BUCKET}/green_ingestion.csv",index=False)
data_set_green.to_gbq(f'dataobservability.green_ingestion',project_id=project,if_exists='replace')

data_set_yellow = pd.read_csv('data/yellow_ingestion.csv',parse_dates=['tpep_pickup_datetime','tpep_dropoff_datetime'])
data_set_yellow.to_csv(f"gs://{BUCKET}/yellow_ingestion.csv",index=False)
data_set_yellow.to_gbq('dataobservability.yellow_ingestion',project_id=project,if_exists='replace')
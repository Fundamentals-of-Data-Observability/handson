import os
import pandas as pd
from google.cloud import bigquery
import pandas_gbq as pgbq


bucket_project = os.environ['GOOGLE_CLOUD_PROJECT']
BUCKET = bucket_project+'_dataobservabilityworkshop'

dataset_file_green = "green_ingestion.csv"
gs_file_uri_green = f"gs://{BUCKET}/{dataset_file_green}"

dataset_file_yellow = "yellow_ingestion.csv"
gs_file_uri_yellow = f"gs://{BUCKET}/{dataset_file_yellow}"

green_df = pd.read_csv(gs_file_uri_green)
yellow_df = pd.read_csv(gs_file_uri_yellow)

pgbq.to_gbq(green_df,'dataobservabilityworkshop.green', project_id=bucket_project, if_exists='replace')
pgbq.to_gbq(yellow_df,'dataobservabilityworkshop.yellow', project_id=bucket_project, if_exists='replace')

query_string = f'''
SELECT 'green' AS Taxi_Color, * FROM (SELECT DISTINCT passenger_count,
    Count(*) as CountTrips
FROM     `{bucket_project}.dataobservabilityworkshop.green`
GROUP BY passenger_count)
UNION ALL
(SELECT 'yellow' AS Taxi_Color, * FROM 
((SELECT DISTINCT passenger_count,
    Count(*) as CountTrips
FROM     `{bucket_project}.dataobservabilityworkshop.yellow`
GROUP BY passenger_count))
 )
 '''

bqclient = bigquery.Client()
result = bqclient.query(query_string).to_dataframe()
print(result)

result.to_csv('../../data/number_trips.csv',index=False)





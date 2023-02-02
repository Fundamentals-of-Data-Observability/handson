import urllib3
urllib3.disable_warnings()


from kensu.utils.kensu_provider import KensuProvider as K

k = K().initKensu(process_name='Copy')

def copy_file(src_file, dst_uri):
    import kensu.pandas as pd
    df = pd.read_csv(src_file, parse_dates=['lpep_dropoff_datetime'], infer_datetime_format=True)
    df.to_csv(dst_uri,index=False)

dataset_url="../../data/green_ingestion.csv"
dataset_file = "green_ingestion.csv"

dst_uri = "../../data/green_ingestion_copy.csv"

copy_file(dataset_url, dst_uri)
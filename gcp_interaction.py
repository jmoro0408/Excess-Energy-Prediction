from io import StringIO
from google.cloud import storage
import pandas as pd
from pathlib import Path
from typing import Union

def list_buckets(project_id:str) -> list[str]:
    storage_client = storage.Client(project=project_id)
    bucket_name_list = []
    buckets = storage_client.list_buckets()
    for bucket in buckets:
        bucket_name_list.append(bucket.name)
    return bucket_name_list

def list_blobs(project_id:str, bucket_name:str) -> list[str]:
    storage_client = storage.Client(project=project_id)
    blob_name_list = []
    blobs = storage_client.list_blobs(bucket_or_name = bucket_name)
    for blob in blobs:
        blob_name_list.append(blob.name)
    return blob_name_list

def write_to_blob_csv(bucket_name:str,
                      df:pd.DataFrame,
                      filepath:Union[str,Path]) -> None:
    """Writes a pandas df to a gcp blob as a csv.

    Args:
        bucket_name (str): Name of bucket to write to
        df (pd.DataFrame): DataFrame to write
        filename (Union[str,Path]): full filepath of the file to be written.
        e.g Raw_Data/EirGrid/ALL/demandActual/2017/test_data.csv
        If the folder being written to does not currently exist it will be created.

    Returns:
        None
    """
    filepath = Path(filepath)
    if filepath.suffix != '.csv':
        filepath = filepath.with_suffix('.csv')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(str(filepath)).upload_from_string(df.to_csv(index = False), 'text/csv')
    print(f'{filepath} uploaded to {bucket}')
    return None



def read_blob(bucket_name:str, blob_name:str):
    """Read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        print(f.read())

def read_blob_to_pandas(bucket_name:str, blob_name:str, **kwargs) -> pd.DataFrame:
    """Read a blob from GCS using file-like IO"""
    # **kwargs to be passed to pd.read_csv()
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    # Read from bucket
    data_str = blob.download_as_text()
    return pd.read_csv(StringIO(data_str), **kwargs)

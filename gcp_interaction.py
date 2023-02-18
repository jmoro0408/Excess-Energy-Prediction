
from google.cloud import storage
import pandas as pd

def list_buckets(project_id:str) -> list[str]:
    storage_client = storage.Client(project=project_id)
    bucket_name_list = []
    buckets = storage_client.list_buckets()
    print("Buckets:")
    for bucket in buckets:
        print(bucket.name)
        bucket_name_list.append(bucket.name)
    return bucket_name_list

def list_blobs(project_id:str, bucket_name:str) -> list[str]:
    storage_client = storage.Client(project=project_id)
    blob_name_list = []
    print("Blobs:")
    blobs = bucket_name.list_blobs()
    for blob in blobs:
        print(blob.name)
        blob_name_list.append(blob.name)
    return blob_name_list

def write_blob(bucket_name:str, blob_name:str):
    """Write to a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    with blob.open("w") as f:
        f.write("Hello world")



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

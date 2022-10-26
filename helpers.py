from genericpath import isfile
import os
from google.cloud import bigquery_datatransfer

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

dts_client = bigquery_datatransfer.DataTransferServiceClient()

def is_file_exist(file_path: str):
    if os.path.exists(file_path):
        return True

    return False
    

def create_dts_s3(display_name: str, dest_dataset: str, dest_table: str, s3_uri: str):
    tf_config = bigquery_datatransfer.TransferConfig(
        display_name=display_name,
        destination_dataset_id=dest_dataset,
        data_source_id="amazon_s3",
        params={
            "destination_table_name_template": dest_table,
            "data_path": s3_uri,
            "access_key_id": ACCESS_KEY_ID,
            "secret_access_key": SECRET_ACCESS_KEY,
            "file_format": "PARQUET",
            "write_disposition": "WRITE_APPEND"
        },
        schedule="every 24 hours"
    )

    task = dts_client.create_transfer_config(
        parent=dts_client.common_location_path(
            project=PROJECT_ID,
            location=LOCATION
        ),
        transfer_config=tf_config
    )

    return task
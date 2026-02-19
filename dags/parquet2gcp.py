from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

default_args = {
    "start_date": datetime(2023, 1, 1),
}

BUCKET_NAME = "airflow-bucket1082"
PARQUET_FILE = "/tmp/output.parquet"
DESTINATION_BLOB_NAME = "data/output.parquet"

@task
def generate_parquet():
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    })
    table = pa.Table.from_pandas(df)
    pq.write_table(table, PARQUET_FILE)

    
@task
def upload_to_gcs():
    hook = GCSHook(gcp_conn_id="GCPBucket")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=DESTINATION_BLOB_NAME,
        filename=PARQUET_FILE,
        mime_type="application/octet-stream"
    )
    # Optional cleanup
    os.remove(PARQUET_FILE)

with DAG("parquet_to_gcs",
         default_args=default_args,
        #  schedule_interval=None,
         catchup=False) as dag:

    task_generate = generate_parquet()
    task_upload =  upload_to_gcs()
    # task_generate = pythonoperator(
    #     task_id="generate_parquet",
    #     python_callable=generate_parquet
    # )

    # task_upload = pythonoperator(
    #     task_id="upload_to_gcs",
    #     python_callable=upload_to_gcs
    # )

    task_generate >> task_upload
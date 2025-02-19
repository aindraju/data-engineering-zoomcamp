import os
from datetime import datetime
import logging
from google.cloud import storage

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

URL_PREFIX = "https://nyc-tlc.s3.amazonaws.com/trip+data"
FILENAME_TEMPLATE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
URL_TEMPLATE = URL_PREFIX + '/' + FILENAME_TEMPLATE
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TABLE_NAME_TEMPLATE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y_%m\') }}'
parquet_file = FILENAME_TEMPLATE.replace('.csv', '.parquet')



def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


dag = DAG(
    "data_ingestion_FHV",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    catchup=True,
    max_active_runs=3
)

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {AIRFLOW_HOME}/{FILENAME_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{FILENAME_TEMPLATE}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{AIRFLOW_HOME}/{parquet_file}",
        },
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": TABLE_NAME_TEMPLATE,
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    delete_csv_parquet_task = BashOperator(
        task_id='delete_csv_parquet_task',
        bash_command= f'rm {AIRFLOW_HOME}/{FILENAME_TEMPLATE} {AIRFLOW_HOME}/{parquet_file}'
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> delete_csv_parquet_task

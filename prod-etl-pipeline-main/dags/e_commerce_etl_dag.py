from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
from airflow.decorators import task,task_group
from airflow.exceptions import AirflowException, AirflowSkipException
import subprocess
from pytz import timezone
import time
from airflow.models import Variable
import json
import logging
import os
import pandas as pd
from faker import Faker
import random 
from google.cloud import storage
from google.oauth2 import service_account
from airflow.operators.empty import EmptyOperator
import pendulum
import sys


import dataproc_utils as dp_utils
import data_genarator as dg


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@task
def genarator(st, **context):
    # Generate data
    print(f"starttime : {st}")
    dag_run_conf = context["dag_run"].conf
    source_path = dag_run_conf["raw_gcs_path"].get("default","repo-prod-raw-stg/genarator_dump")
    #project_id = dag_run_conf["project_id"].get("project_id","pro-env-test")
    project_id = "pro-env-test"
    # --- OPTIONAL: Service Account Authentication ---
    # for service account, set the path to service account JSON key file.
    GCS_SERVICE_ACCOUNT_KEY_PATH = None # e.g., 'path/to/service-account-key.json'
    NUM_CUSTOMERS = 500
    NUM_PRODUCTS = 75
    NUM_ORDERS = 100000
    logging.info(source_path,project_id)
    GCS_BUCKET_NAME,fake_file_path  = source_path.split('/',1)
    DATA_DIR = source_path.split('/',1)[1]
    if not GCS_BUCKET_NAME or not DATA_DIR:
        raise AirflowException("It is mandatory to pass gcs path")

    customers_df, products_df, orders_df = dg.generate_fake_data(NUM_CUSTOMERS,NUM_PRODUCTS,NUM_ORDERS)

    # Save data locally
    dg.save_data_to_csv(customers_df, products_df, orders_df,DATA_DIR)

    # Upload data to GCS
    try:
        dg.upload_to_gcs(GCS_BUCKET_NAME,fake_file_path, DATA_DIR, project_id=project_id,
                      service_account_key_path=GCS_SERVICE_ACCOUNT_KEY_PATH)
    except Exception as e:
        logging.info(f"An error occurred during GCS upload. Please ensure your bucket name, project ID, and authentication are correctly configured.")
        logging.info(f"Error: {e}")

@task
def _submit_gcs_to_bq_job_task(**context):

    dag_run_conf = context["dag_run"].conf
    gcs_input_path = dag_run_conf["gcs_input_path"].get("default")
    bq_target_table_name = dag_run_conf["bq_target_table_name"].get("default")
    bq_target_dataset_name = dag_run_conf["bq_target_dataset_name"].get("default")
    bq_target_project_id = dag_run_conf["bq_target_project_id"].get("default")
    gcs_temp_bucket = dag_run_conf["gcs_temp_bucket"].get("default")
    input_format = dag_run_conf["input_format"].get("default")
    header = dag_run_conf["header"].get("default")
    infer_schema = "true"


    submit_cmd = dp_utils.submit_dataproc_gcs_to_bq_job_cmd(
        gcs_input_path,
        bq_target_dataset_name,
        bq_target_table_name,
        bq_target_project_id,
        gcs_temp_bucket,
        input_format,
        header,
        infer_schema
    )
    dp_utils.run_gcloud_command(submit_cmd, success_message_contains="Job submitted")

@task
def _create_cluster():
    create_cmd = dp_utils.create_dataproc_cluster_cmd()
    dp_utils.run_gcloud_command(create_cmd, success_message_contains="Cluster created.")

@task
def _delete_cluster():
    delete_cmd = dp_utils.delete_dataproc_cluster_cmd()
    dp_utils.run_gcloud_command(delete_cmd, success_message_contains="Deleted cluster")


defaulta_args = {
    "owner":"e-commerce",
    "start_date" : pendulum.datetime(2023, 1, 1, tz="UTC"),
    "retries":0
}

with DAG(
    dag_id = "e-commerce_data_flow",
    default_args = defaulta_args,
    catchup=False,
    schedule= None,
    params={ 

        "raw_gcs_path": {
            "type": "string",
            "title": "Raw source",
            "description": "GCS bucket where you want to load genarated data.",
            "default": "repo-prod-raw-stg/genarator_dump",
        },
        "gcs_input_path": {
            "type": "string",
            "title": "Source File Name",
            "description": "Name of the file to extract from gcs to big query (e.g., data.csv, sales.json).",
            "default": None,
        },
        "bq_target_dataset_name": {
            "type": "string",
            "title": "Target dataset name",
            "description": "Name of the big query destination dataset",
            "default": None,
        },
        "bq_target_table_name": {
            "type": "string",
            "title": "Target table name",
            "description": "Name of the big query destination table",
            "default": None,
        },
        "bq_target_project_id": {
            "type": "string",
            "title": "project id",
            "description": "Name of the big query project id",
            "default": None,
        },
        "gcs_temp_bucket": {
            "type": "string",
            "title": "tmp gcs path",
            "description": "Temporary GCS bucket for BigQuery connector like staging area",
            "default": None,
        },
        "input_format": {
            "type": "string",
            "title": "file format",
            "description": "file format of the source gcs files like csv,json..",
            "default": None,
        },
        "header": {
            "type": "boolean",
            "title": "header",
            "description": "is header available in the source files",
            "default": "true",
        },
    }

):
    tz = timezone('UTC')
    st = datetime.now(tz).replace(microsecond=0).replace(tzinfo=None)
    extract = genarator(st)
    cluster_up = _create_cluster()
    submit_job = _submit_gcs_to_bq_job_task()
    cluster_down = _delete_cluster()

    extract >> cluster_up >> submit_job >> cluster_down
        

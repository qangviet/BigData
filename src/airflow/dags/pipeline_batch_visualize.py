import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pickle

load_dotenv()
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)

from batch_processing.raw_to_processed_2 import Config as Config_1, raw_to_processed
from batch_processing.load_and_transform import Config as Config_2, load_transform_save


BOOTSTRAP_SERVERS = ["broker:29092"]

JARS_DIR = "/opt/airflow/jars"
DATA_DIR = "/opt/airflow/data"

PATH_AUTH_FILE = os.path.join(DATA_DIR, "auth_gg_bigquery.json")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = PATH_AUTH_FILE

YEAR = os.getenv("YEAR_TEST")

MONTH = os.getenv("MONTH_TEST")

DAY = os.getenv("DAY_TEST")

DATE = f"{YEAR}-{MONTH}-{DAY}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BUCKET_NAME_1 = os.getenv("BUCKET_NAME_1")

BUCKET_NAME_2 = os.getenv("BUCKET_NAME_2")

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")

BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")

args_raw_to_processed = Config_1(
    minio_access_key=MINIO_ACCESS_KEY,
    minio_secret_key=MINIO_SECRET_KEY,
    endpoint_url=MINIO_ENDPOINT,
    jars_dir=JARS_DIR,
    taxi_lookup_path=f"s3a://{BUCKET_NAME_1}/taxi_lookup.csv",
    num_output_files=1,
    green_path_src=f"s3a://{BUCKET_NAME_1}/cdc_db/{YEAR}/{MONTH}/{DAY}/data",
    yellow_path_src=f"s3a://{BUCKET_NAME_1}/yellow/{YEAR}/{MONTH}/{DAY}.parquet",
    green_path_dist=f"s3a://{BUCKET_NAME_2}/data/green/{YEAR}/{MONTH}/{DAY}",
    yellow_path_dist=f"s3a://{BUCKET_NAME_2}/data/yellow/{YEAR}/{MONTH}/{DAY}",
)

args_spark_to_bigquery = Config_2(
    max_memory="2g",
    minio_access_key=MINIO_ACCESS_KEY,
    minio_secret_key=MINIO_SECRET_KEY,
    minio_endpoint=MINIO_ENDPOINT,
    jars_dir=JARS_DIR,
    bucket_name=BUCKET_NAME_2,
    year=YEAR,
    month=MONTH,
    day=DAY,
    date=DATE,
    taxi_lookup_path=f"s3a://{BUCKET_NAME_1}/taxi_lookup.csv",
    path_auth_file=PATH_AUTH_FILE,
)


# def test_interact_with_windows():
#     windows_path = "/mnt/host/e/test.txt"
#     with open(windows_path, "w") as f:
#         f.write("Hello from Windows!")
#     logging.info("Write to Windows path successfully!")


def start_pipeline():
    logging.info("Start pipeline:", DATE)
    logging.info("Args raw to processed:", args_raw_to_processed)
    logging.info("Args spark to bigquery:", args_spark_to_bigquery)

    if not os.path.exists(PATH_AUTH_FILE):
        logging.info("Path auth not exist")
    else:
        logging.info("Path auth exist")
    logging.info(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    # test_interact_with_windows()


with DAG(
    "pipeline_batch_visualize",
    description="Pipeline for batch processing",
    schedule_interval=None,
    tags=["batch"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_pipeline,
    )

    raw_to_processed_op = PythonOperator(
        task_id="raw_to_processed",
        python_callable=raw_to_processed,
        op_args=[args_raw_to_processed],
    )

    transform_save_to_bg_op = PythonOperator(
        task_id="transform_save_to_bigquery",
        python_callable=load_transform_save,
        op_args=[args_spark_to_bigquery],
    )

    end = DummyOperator(task_id="end")
    start >> raw_to_processed_op >> transform_save_to_bg_op >> end

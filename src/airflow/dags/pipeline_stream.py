import os
import psycopg2
import datetime
from airflow.sensors.filesystem import FileSensor
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import sys

from stream_processing.insert_data_to_db import (
    simulate_data_with_image,
    simulate_data,
    Args as simulate_args,
)
from stream_processing.spark_streaming_to_dl import (
    run_all,
    Args as spark_streaming_args,
)
from stream_processing.streaming_image_to_dl import (
    image_to_dl,
    Args as image_args,
)
from stream_processing.streaming_speech_to_dl import (
    speech_to_dl,
    Args as speech_args,
)

load_dotenv()

PROJECT_ROOT = os.environ["PROJECT_BIGDATA_ROOT"]

BOOTSTRAP_SERVERS = ["broker:29092"]

DATA_DIR = "/opt/airflow/data"

JARS_DIR = "/opt/airflow/jars"

YEAR = os.getenv("YEAR_TEST")

MONTH = os.getenv("MONTH_TEST")

DAY = os.getenv("DAY_TEST")

FILE_TEST = os.path.join(DATA_DIR, f"new_2/{YEAR}/Green/{MONTH}/{DAY}.parquet")

TABLE_GREEN = os.getenv("TABLE_NAME_CDC_GREEN") + f"_{YEAR}_{MONTH}_{DAY}"

TOPIC_CDC_DB = "streaming.public." + TABLE_GREEN

IMAGE_TEST = os.path.join(DATA_DIR, os.getenv("IMAGE_TEST"))

SPEECH_TEST = os.path.join(DATA_DIR, os.getenv("SPEECH_TEST"))

KAFKA_TOPIC_IMAGE = os.getenv("KAFKA_TOPIC_IMAGE") + f"_{YEAR}_{MONTH}_{DAY}"

KAFKA_TOPIC_SPEECH = os.getenv("KAFKA_TOPIC_SPEECH") + f"_{YEAR}_{MONTH}_{DAY}"

MAX_MESSAGE_SIZE = 1024 * 1024 * 5  # 5MB

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

DB_ARGS = {
    "dbname": os.getenv("DB_STREAM_NAME"),
    "user": os.getenv("DB_STREAM_USER"),
    "password": os.getenv("DB_STREAM_PASSWORD"),
    "host": os.getenv("DB_STREAM_HOST"),
    "port": os.getenv("DB_STREAM_PORT"),
}

args_simulation = simulate_args(
    table_name=TABLE_GREEN,
    time_sleep=2,
    file_path=FILE_TEST,
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
    topic_image=KAFKA_TOPIC_IMAGE,
    topic_speech=KAFKA_TOPIC_SPEECH,
    max_message_size=MAX_MESSAGE_SIZE,
    clear_table_first=False,
    image_path=IMAGE_TEST,
    speech_path=SPEECH_TEST,
)
args_spark_streaming = spark_streaming_args(
    jars_dir=JARS_DIR,
    topic_cdc_db=TOPIC_CDC_DB,
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
    bucket_name="raw",
    year=YEAR,
    month=MONTH,
    day=DAY,
    minio_access_key=MINIO_ACCESS_KEY,
    minio_secret_key=MINIO_SECRET_KEY,
    minio_endpoint=MINIO_ENDPOINT,
)
args_image_streaming = image_args(
    minio_endpoint=MINIO_ENDPOINT,
    minio_access_key=MINIO_ACCESS_KEY,
    minio_secret_key=MINIO_SECRET_KEY,
    bucket_name="raw",
    prefix="image",
    kafka_topic=KAFKA_TOPIC_IMAGE,
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
)
args_speech_streaming = speech_args(
    minio_endpoint=MINIO_ENDPOINT,
    minio_access_key=MINIO_ACCESS_KEY,
    minio_secret_key=MINIO_SECRET_KEY,
    bucket_name="raw",
    prefix="speech",
    kafka_topic=KAFKA_TOPIC_SPEECH,
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
)


def start():
    print("Hello from func_1")

    print("Root: ", PROJECT_ROOT)
    print("File test: ", FILE_TEST)
    print("Args simulation: ", args_simulation)
    print("Args spark streaming: ", args_spark_streaming)
    print("Args image streaming: ", args_image_streaming)
    print("Args speech streaming: ", args_speech_streaming)


# def simulate_data_with_image_signal(conn, args, signal_file):
#     """
#     Wrapper function to include file deletion
#     """
#     if os.path.exists(signal_file):
#         os.remove(signal_file)
#         print(f"Removed existing signal file: {signal_file}")
#     simulate_data_with_image(conn, args)
#     # Add signal creation after simulation
#     with open(signal_file, "w") as f:
#         f.write("done")


with DAG(
    "streaming_pipeline",
    schedule_interval=None,
    tags=["streaming", "qviet"],
    start_date=datetime.datetime(2022, 1, 1),
) as dag:

    start_pipline = PythonOperator(task_id="start", python_callable=start)

    simulate_data_image_speech = PythonOperator(
        task_id="simulation_data_with_image_speech",
        python_callable=simulate_data_with_image,
        op_args=[DB_ARGS, args_simulation],
    )
    cdc_spark_streaming = PythonOperator(
        task_id="cdc_spark_streaming_to_datalake",
        python_callable=run_all,
        op_args=[args_spark_streaming],
    )
    image_streaming = PythonOperator(
        task_id="image_to_datalake",
        python_callable=image_to_dl,
        op_args=[args_image_streaming],
    )
    speech_streaming = PythonOperator(
        task_id="speech_to_datalake",
        python_callable=speech_to_dl,
        op_args=[args_speech_streaming],
    )
    # dag_1 = PythonOperator(
    #     task_id="func_1",
    #     python_callable=func_1,
    # )

    start_pipline >> simulate_data_image_speech
    start_pipline >> cdc_spark_streaming
    start_pipline >> speech_streaming
    start_pipline >> image_streaming

with DAG(
    "streaming_pipeline_only_data",
    schedule_interval=None,
    tags=["streaming", "qviet"],
    start_date=datetime.datetime(2022, 1, 1),
) as dag_2:

    start_pipline_2 = PythonOperator(task_id="start", python_callable=start)

    simulate_data_only_db = PythonOperator(
        task_id="simulation_data_with_image_speech",
        python_callable=simulate_data,
        op_args=[DB_ARGS, args_simulation],
    )
    cdc_spark_streaming_2 = PythonOperator(
        task_id="cdc_spark_streaming_to_datalake",
        python_callable=run_all,
        op_args=[args_spark_streaming],
    )

    start_pipline_2 >> simulate_data_only_db
    start_pipline_2 >> cdc_spark_streaming_2
# py ./src/airflow/dags/pipline_stream.py

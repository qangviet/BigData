import os
import psycopg2
from dotenv import load_dotenv
from airflow.sensors.filesystem import FileSensor

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import sys

# Streaming
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
load_dotenv()
from modules.insert_data_to_db import (
    simulate_data_with_image,
    Args as simulate_args,
)
from modules.spark_streaming_to_dl import (
    run_all,
    Args as spark_streaming_args,
)
from modules.streaming_image_to_dl import (
    image_to_dl,
    Args as image_args,
)
from modules.streaming_speech_to_dl import (
    speech_to_dl,
    Args as speech_args,
)

BOOTSTRAP_SERVERS = ["localhost:9092"]

DATA_DIR = os.path.join(PROJECT_ROOT, "data")

FILE_TEST = os.path.join(DATA_DIR, os.getenv("FILE_TEST_STREAMING"))

YEAR = os.getenv("YEAR_TEST")

MONTH = os.getenv("MONTH_TEST")
TABLE_GREEN = os.getenv("TABLE_NAME_CDC_GREEN")

TABLE_YELLOW = os.getenv("TABLE_NAME_CDC_YELLOW")

IMAGE_TEST = os.path.join(DATA_DIR, os.getenv("IMAGE_TEST"))

SPEECH_TEST = os.path.join(DATA_DIR, os.getenv("SPEECH_TEST"))

KAFKA_TOPIC_IMAGE = os.getenv("KAFKA_TOPIC_IMAGE")

KAFKA_TOPIC_SPEECH = os.getenv("KAFKA_TOPIC_SPEECH")

MAX_MESSAGE_SIZE = 1024 * 1024 * 5  # 5MB

SIGNAL_FILE = os.path.join(PROJECT_ROOT, "/tmp/simulate_data_done.txt")

conn = psycopg2.connect(
    dbname=os.getenv("DB_STREAM_NAME"),
    user=os.getenv("DB_STREAM_USER"),
    password=os.getenv("DB_STREAM_PASSWORD"),
    host=os.getenv("DB_STREAM_HOST"),
    port=os.getenv("DB_STREAM_PORT"),
)

args_simulation = simulate_args(
    table_name=TABLE_GREEN,
    time_sleep=5,
    file_path=FILE_TEST,
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
    topic_image=KAFKA_TOPIC_IMAGE,
    topic_speech=KAFKA_TOPIC_SPEECH,
    max_message_size=MAX_MESSAGE_SIZE,
    signal_file=SIGNAL_FILE,
)
args_spark_streaming = spark_streaming_args(
    jars_dir=os.path.join(PROJECT_ROOT, "jars"),
    topic_cdc_db="streaming.public.green_trip_raw",
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
    bucket_name="raw",
    year=YEAR,
    month=MONTH,
)
args_image_streaming = image_args(
    minio_endpoint=os.getenv("MINIO_ENDPOINT"),
    minio_access_key=os.getenv("MINIO_ACCESS_KEY"),
    minio_secret_key=os.getenv("MINIO_SECRET_KEY"),
    bucket_name=os.getenv("BUCKET_NAME"),
    prefix="image",
    kafka_topic=KAFKA_TOPIC_IMAGE,
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
)
args_speech_streaming = speech_args(
    minio_endpoint=os.getenv("MINIO_ENDPOINT"),
    minio_access_key=os.getenv("MINIO_ACCESS_KEY"),
    minio_secret_key=os.getenv("MINIO_SECRET_KEY"),
    bucket_name=os.getenv("BUCKET_NAME"),
    prefix="speech",
    kafka_topic=KAFKA_TOPIC_SPEECH,
    bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
)


def simulate_data_with_image_signal(conn, args, signal_file):
    """
    Wrapper function to include file deletion
    """
    if os.path.exists(signal_file):
        os.remove(signal_file)
        print(f"Removed existing signal file: {signal_file}")
    simulate_data_with_image(conn, args)
    # Add signal creation after simulation
    with open(signal_file, "w") as f:
        f.write("done")


with DAG(
    "streaming_pipeline",
    schedule_interval=None,
    tags=["streaming", "qviet"],
) as dag:

    start = DummyOperator(task_id="start")

    simulate_data = PythonOperator(
        task_id="simulation_data",
        python_callable=simulate_data_with_image_signal,
        op_args=[conn, args_simulation],
    )
    cdc_spark_streaming = PythonOperator(
        task_id="cdc_spark_streaming",
        python_callable=run_all,
        op_args=[args_spark_streaming],
    )
    image_streaming = PythonOperator(
        task_id="image_streaming",
        python_callable=image_to_dl,
        op_args=[args_image_streaming],
    )
    speech_streaming = PythonOperator(
        task_id="speech_streaming",
        python_callable=speech_to_dl,
        op_args=[args_speech_streaming],
    )

    end = DummyOperator(task_id="end")

    start >> simulate_data
    simulate_data >> [cdc_spark_streaming, image_streaming, speech_streaming]
    [cdc_spark_streaming, image_streaming, speech_streaming] >> end

# py ./src/airflow/dags/pipline_stream.py

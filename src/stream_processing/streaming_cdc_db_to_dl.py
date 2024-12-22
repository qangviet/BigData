from dotenv import load_dotenv
import os
import logging
import traceback
import json
from utils.helpers import load_cfg
from utils.minio_utils import MinIOClient
from kafka import KafkaConsumer
import warnings
from deltalake import write_deltalake
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType,
)
import time
from delta.pip_utils import configure_spark_with_delta_pip

load_dotenv()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)
warnings.filterwarnings("ignore")


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

CFG_DL_PATH = os.path.join(PROJECT_ROOT, "config", "datalake.yaml")
datalake_cfg = load_cfg(CFG_DL_PATH)["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]

BUCKET_NAME = "raw"

# BUCKET_NAME_3 = datalake_cfg['bucket_name_3']

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
JARS_DIR = os.path.join(PROJECT_ROOT, "jars")


def convert_timestamp_to_datetime(timestamp: int) -> str:
    """
    Chuyển đổi timestamp (dạng int) sang dạng thời gian dễ đọc.

    Args:
        timestamp (int): Giá trị timestamp (Unix time).

    Returns:
        str: Chuỗi thời gian ở định dạng "YYYY-MM-DD HH:MM:SS".
    """

    try:
        # Chuyển timestamp sang đối tượng datetime

        timestamp = int(timestamp / 1_000_000)
        timestamp = timestamp - 7 * 3600
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        # utc_time = dt_object.replace(tzinfo=pytz.UTC)
        # Định dạng datetime thành chuỗi
        return dt_object.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        return f"Error: {e}"


def check_jars(jars):
    """
    Check if the JAR files exist
    """
    for jar in jars:
        if not os.path.exists(jar):
            logging.error(f"JAR file {jar} does not exist!")
            return False

    return True


def create_spark_session():
    spark_jars = [
        JARS_DIR + "/hadoop-aws-3.3.4.jar",
        JARS_DIR + "/delta-core_2.12-2.4.0.jar",
        JARS_DIR + "/aws-java-sdk-bundle-1.12.262.jar",
    ]
    print("Checking JAR files...: ", check_jars(spark_jars))
    builder = (
        SparkSession.builder.appName("KafkaDeltaMinIO")
        .config("spark.jars", ",".join(spark_jars))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logging.info("Spark session successfully created!")

    return spark


schema = StructType(
    [
        StructField("vendorid", IntegerType(), True),
        StructField("lpep_pickup_datetime", StringType(), True),
        StructField("lpep_dropoff_datetime", StringType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("ratecodeid", DoubleType(), True),
        StructField("pulocationid", IntegerType(), True),
        StructField("dolocationid", IntegerType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("ehail_fee", StringType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", DoubleType(), True),
        StructField("trip_type", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("id", StringType(), True),
        StructField("id_customer", StringType(), True),
    ]
)
MINIO_PATH = f"s3a://{BUCKET_NAME}/stream/green_trip_raw"


if __name__ == "__main__":
    spark_ss = create_spark_session()
    minio_client = None
    try:
        for _ in range(10):
            consumer = KafkaConsumer(
                "streaming.public.green_trip_raw",
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="latest",  # Hoặc 'latest'
            )
            break
        logging.info("Consumer created successfully!")
    except Exception as e:
        logging.error(f"Error: {e}")
        traceback.print_exc()
        exit(1)
    empty_df = spark_ss.createDataFrame(spark_ss.sparkContext.emptyRDD(), schema=schema)
    print("Start consuming data...")
    record_count = 0
    upload_interval = 10  # Số lượng bản ghi sau đó mới upload
    upload_time = time.time()  # Thời gian bắt đầu
    df_buffer = empty_df
    try:
        for message in consumer:
            record = message.value

            # Tạo DataFrame từ bản ghi Kafka
            payload = record.get("payload", {})

            # Tách dữ liệu bản ghi mới
            after_data = payload.get("after", None)

            if after_data:
                pickup_timestamp_sec = convert_timestamp_to_datetime(
                    after_data["lpep_pickup_datetime"]
                )
                dropoff_timestamp_sec = convert_timestamp_to_datetime(
                    after_data["lpep_dropoff_datetime"]
                )
                row_df = spark_ss.createDataFrame(
                    [
                        (
                            after_data["vendorid"],
                            pickup_timestamp_sec,
                            dropoff_timestamp_sec,
                            after_data["store_and_fwd_flag"],
                            after_data["ratecodeid"],
                            after_data["pulocationid"],
                            after_data["dolocationid"],
                            after_data["passenger_count"],
                            after_data["trip_distance"],
                            after_data["fare_amount"],
                            after_data["extra"],
                            after_data["mta_tax"],
                            after_data["tip_amount"],
                            after_data["tolls_amount"],
                            (
                                str(after_data["ehail_fee"])
                                if after_data["ehail_fee"] is not None
                                else None
                            ),
                            after_data["improvement_surcharge"],
                            after_data["total_amount"],
                            after_data["payment_type"],
                            after_data["trip_type"],
                            after_data["congestion_surcharge"],
                            after_data["id"],
                            after_data["id_customer"],
                        )
                    ],
                    schema=schema,
                )
            # Append dữ liệu vào DataFrame buffer
            df_buffer = df_buffer.union(row_df)
            record_count += 1
            print(f"Appended record to buffer. Total records: {record_count}")
            # Ghi vào Delta Lake trên MinIO
            if record_count >= upload_interval or time.time() - upload_time >= 30:
                # Upload lên MinIO
                df_buffer.write.format("parquet").mode("append").save(MINIO_PATH)
                print(f"Wrote to Delta Lake on MinIO: {MINIO_PATH} at {datetime.now()}")
                # Reset DataFrame và biến đếm
                df_buffer = empty_df
                record_count = 0
                upload_time = time.time()
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        spark_ss.stop()

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from dotenv import load_dotenv
import os
import logging
import traceback
import json
from utils.helpers import load_cfg
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    FloatType,
    DoubleType,
)

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

BUCKET_NAME = datalake_cfg["bucket_name_1"]
YEAR = os.getenv("YEAR_TEST")
MONTH = os.getenv("MONTH_TEST")
JARS_DIR = os.path.join(PROJECT_ROOT, "jars")

BOOTSTRAP_SERVERS = ["localhost:9092"]


def check_jars(jars):
    """
    Check if the JAR files exist
    """
    for jar in jars:
        if not os.path.exists(jar):
            logging.error(f"JAR file {jar} does not exist!")
            return False

    return True


###############################################
# PySpark
###############################################
def create_spark_session():
    """
    Create the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    spark_jars = [
        JARS_DIR + "/hadoop-aws-3.3.4.jar",
        # JARS_DIR + "/spark-sql-kafka-0-10_2.12-3.5.3.jar",
        # JARS_DIR + "/kafka-clients-3.4.0.jar",
        JARS_DIR + "/aws-java-sdk-bundle-1.12.262.jar",
        # JARS_DIR + "/spark-streaming-kafka-0-10_2.12-3.5.3.jar",
    ]
    print("Checking JAR files...: ", check_jars(spark_jars))
    spark = None
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.config("spark.executor.memory", "4g")
            .config("spark.jars", ",".join(spark_jars))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # .config("spark.streaming.kafka.maxRatePerPartition", "100")
            # .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.consumer.cache.enabled", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            # .config("spark.master", "local[*]")
            # .config("spark.hadoop.io.native.lib.available", "false")
            # .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.parquet.writeLegacyFormat", "true")
            .appName("Streaming Processing Application")
        )
        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=[
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
            ],
        ).getOrCreate()

        logging.info("Spark session successfully created!")

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def load_minio_config(spark_context: SparkContext):
    """
    Establist the necessary connection to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.access.key", MINIO_ACCESS_KEY
        )
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.secret.key", MINIO_SECRET_KEY
        )
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.connection.ssl.enabled", "false"
        )
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        logging.info("MinIO configuration is created successfully")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the MinIO configuration due to exception: {e}")


def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly
    """
    topic_cdc_db = "streaming.public.green_trip_raw"
    try:
        df = (
            spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", ",".join(BOOTSTRAP_SERVERS))
            .option("subscribe", topic_cdc_db)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("Initial dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Initial dataframe could not be created due to exception: {e}")

    return df


schema = StructType(
    [
        StructField("vendorid", IntegerType(), True),
        StructField("lpep_pickup_datetime", LongType(), True),
        StructField("lpep_dropoff_datetime", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("ratecodeid", FloatType(), True),
        StructField("pulocationid", IntegerType(), True),
        StructField("dolocationid", IntegerType(), True),
        StructField("passenger_count", FloatType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("ehail_fee", StringType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", FloatType(), True),
        StructField("trip_type", FloatType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("id", StringType(), True),
        StructField("id_customer", StringType(), True),
    ]
)


def create_final_dataframe(kafka_df, spark_session):

    json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(
        from_json(col("value"), schema).alias("data")
    )
    parsed_df = json_df.withColumn(
        "lpep_pickup_datetime",
        (col("data.lpep_pickup_datetime") / 1000000).cast("timestamp"),
    ).withColumn(
        "lpep_dropoff_datetime",
        (col("data.lpep_dropoff_datetime") / 1000000).cast("timestamp"),
    )
    parsed_df.createOrReplaceTempView("nyc_taxi_view")

    df_final = spark.sql(
        """
        SELECT
            * 
        FROM nyc_taxi_view
    """
    )

    logging.info("Final dataframe created successfully!")
    return df_final


def start_steaming(df):
    """
    Store data into Datalake (MinIO) with parquet format
    """
    logging.info("Streaming is being started...")
    stream_query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("path", f"s3a://{BUCKET_NAME}/cdc_db/data/{YEAR}/{MONTH}")
        .option(
            "checkpointLocation",
            f"s3a://{BUCKET_NAME}/cdc_db/checkpoint/{YEAR}/{MONTH}",
        )
        .start()
    )
    return stream_query.awaitTermination()


if __name__ == "__main__":
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
    kafka_df = create_initial_dataframe(spark)

    df_final = create_final_dataframe(kafka_df, spark)
    start_steaming(df_final)
# py ./src/stream_processing/spark_streaming_to_dl.py

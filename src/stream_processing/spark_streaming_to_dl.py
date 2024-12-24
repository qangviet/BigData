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
    BooleanType,
)
from dataclasses import dataclass

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
DAY = os.getenv("DAY_TEST")

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


@dataclass
class Args:
    jars_dir: str = JARS_DIR
    bootstrap_servers: str = ".".join(BOOTSTRAP_SERVERS)
    bucket_name: str = BUCKET_NAME
    year: str = YEAR
    month: str = MONTH
    day: str = DAY
    topic_cdc_db: str = "streaming.public.green_trip_raw" + f"_{YEAR}_{MONTH}_{DAY}"
    minio_access_key: str = MINIO_ACCESS_KEY
    minio_secret_key: str = MINIO_SECRET_KEY
    minio_endpoint: str = MINIO_ENDPOINT


###############################################
# PySpark
###############################################
def create_spark_session(jars_dir):
    """
    Create the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    spark_jars = [
        jars_dir + "/hadoop-aws-3.3.4.jar",
        jars_dir + "/aws-java-sdk-bundle-1.12.262.jar",
    ]
    print("Checking JAR files...: ", check_jars(spark_jars))
    spark = None
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.config("spark.executor.memory", "2g")
            .config("spark.jars", ",".join(spark_jars))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.debug.maxToStringFields", 1000)
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


def load_minio_config(
    spark_context: SparkContext, minio_access_key, minio_secret_key, minio_endpoint
):
    """
    Establist the necessary connection to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.access.key", minio_access_key
        )
        spark_context._jsc.hadoopConfiguration().set(
            "fs.s3a.secret.key", minio_secret_key
        )
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
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


def create_initial_dataframe(spark_session, topic_cdc, bootstrap_servers):
    """
    Reads the streaming data and creates the initial dataframe accordingly
    """
    try:
        df = (
            spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic_cdc)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("Initial dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Initial dataframe could not be created due to exception: {e}")

    return df


after_schema = StructType(
    [
        StructField("vendorid", IntegerType(), True),
        StructField(
            "lpep_pickup_datetime", LongType(), True
        ),  # Use LongType for timestamps as microseconds
        StructField(
            "lpep_dropoff_datetime", LongType(), True
        ),  # Use LongType for timestamps as microseconds
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
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", DoubleType(), True),
        StructField("trip_type", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("id", StringType(), True),
        StructField(
            "id_customer", LongType(), True
        ),  # Use LongType as id_customer is long
        StructField("rate", DoubleType(), True),
    ]
)


def create_final_dataframe(kafka_df, spark_session):

    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
    # Giải mã JSON và áp dụng schema
    parsed_df = kafka_df.select(
        from_json(
            col("value"),
            StructType(
                [
                    StructField(
                        "schema",
                        StructType(
                            [
                                StructField("type", StringType(), True),
                                StructField(
                                    "fields",
                                    ArrayType(
                                        StructType(
                                            [
                                                StructField("type", StringType(), True),
                                                StructField(
                                                    "optional", BooleanType(), True
                                                ),
                                                StructField(
                                                    "field", StringType(), True
                                                ),
                                                StructField("name", StringType(), True),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "payload",
                        StructType(
                            [
                                StructField("before", StructType(), True),
                                StructField("after", after_schema, True),
                                StructField("source", StructType(), True),
                                StructField("transaction", StructType(), True),
                                StructField("op", StringType(), True),
                                StructField("ts_ms", LongType(), True),
                                StructField("ts_us", LongType(), True),
                                StructField("ts_ns", LongType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
        ).alias("data")
    ).select("data.payload.after.*")

    # Bây giờ bạn có thể xử lý parsed_df như một DataFrame thông thường
    # parsed_df = parsed_df.withColumn(
    #     "lpep_pickup_datetime",
    #     (col("data.lpep_pickup_datetime") / 1000000).cast("timestamp"),
    # ).withColumn(
    #     "lpep_dropoff_datetime",
    #     (col("data.lpep_dropoff_datetime") / 1000000).cast("timestamp"),
    # )
    parsed_df = parsed_df.select(
        "vendorid",
        (col("lpep_pickup_datetime") / 1000000)
        .cast("timestamp")
        .alias("lpep_pickup_datetime"),
        (col("lpep_dropoff_datetime") / 1000000)
        .cast("timestamp")
        .alias("lpep_dropoff_datetime"),
        "store_and_fwd_flag",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "trip_type",
        "congestion_surcharge",
        "id",
        "id_customer",
        "rate",
    )
    parsed_df.printSchema()
    count_df = parsed_df.groupBy("id_customer").count().alias("customer_count")

    logging.info("Final dataframe created successfully!")
    return parsed_df, count_df


def start_steaming(df, count_df, bucket_name, year, month, day):
    """
    Store data into Datalake (MinIO) with parquet format
    """
    logging.info("Streaming is being started...")
    stream_query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("path", f"s3a://{bucket_name}/cdc_db/{year}/{month}/{day}/data")
        .option(
            "checkpointLocation",
            f"s3a://{bucket_name}/cdc_db/{year}/{month}/{day}/checkpoint",
        )
        .start()
    )
    count_stream_query = (
        count_df.writeStream.format("delta")
        .outputMode("complete")  # Complete vì bạn muốn ghi toàn bộ bảng đếm
        .option(
            "path", f"s3a://{bucket_name}/cdc_db/{year}/{month}/{day}/count_customer"
        )
        .option(
            "checkpointLocation",
            f"s3a://{bucket_name}/cdc_db/{year}/{month}/{day}/count_checkpoint",
        )
        .start()
    )

    return stream_query.awaitTermination(), count_stream_query.awaitTermination()


def run_all(args: Args):
    spark = create_spark_session(args.jars_dir)

    load_minio_config(
        spark.sparkContext,
        args.minio_access_key,
        args.minio_secret_key,
        args.minio_endpoint,
    )
    kafka_df = create_initial_dataframe(
        spark, args.topic_cdc_db, args.bootstrap_servers
    )
    df_final, df_count = create_final_dataframe(kafka_df, spark)
    start_steaming(
        df_final, df_count, args.bucket_name, args.year, args.month, args.day
    )


if __name__ == "__main__":
    args = Args()
    run_all(args)

# py ./src/stream_processing/spark_streaming_to_dl.py

import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
import yaml
from pyspark import SparkConf, SparkContext
from google.cloud import bigquery
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    TimestampNTZType,
)


from minio import Minio
from pyspark.sql import functions as F

dotenv.load_dotenv()


def load_cfg(cfg_file):
    """Load configuration from a YAML config file"""
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)
    return cfg


class MinIOClient:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key

    def create_conn(self):
        client = Minio(
            endpoint=self.endpoint_url,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )
        return client

    def create_bucket(self, bucket_name):
        client = self.create_conn()
        found = client.bucket_exists(bucket_name=bucket_name)
        if not found:
            client.make_bucket(bucket_name=bucket_name)
            print(f"Bucket {bucket_name} created successfully!")
        else:
            print(f"Bucket {bucket_name} already exists, skip creating!")

    def list_parquet_files(self, bucket_name, prefix=""):
        client = self.create_conn()
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        parquet_files = [
            obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
        ]
        return parquet_files


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)
warnings.filterwarnings("ignore")


def check_jars(jars):
    """Check if the JAR files exist"""
    for jar in jars:
        if not os.path.exists(jar):
            logging.error(f"JAR file {jar} does not exist!")
            return False
    return True


def load_data_from_minio(spark, bucket_name, date, taxi_type):
    """
    params:
    BUCKET_NAME: tên bucket
    YEAR: năm
    TAXI_TYPE: loại xe  Green, Yellow
    return:
    df_data: dữ liệu
    """

    year = date.split("-")[0]
    month = date.split("-")[1]
    day = date.split("-")[2]

    path = f"s3a://{bucket_name}/data/{taxi_type}/{year}/{month}/{day}"

    df = spark.read.parquet(path)
    df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))

    return df


def load_data_from_minio_for_visualize(spark, bucket_name, date, taxi_type):

    year = date.split("-")[0]
    month = date.split("-")[1]
    day = date.split("-")[2]
    path_list = []
    combined_df = None

    for i in range(1, int(day) + 1):

        path = f"s3a://{bucket_name}/data/{taxi_type}/{year}/{month}/{i:02}"
        path_list.append(path)
    for path in path_list:
        # Đọc dữ liệu Delta từ thư mục con
        df = spark.read.parquet(path)
        df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))

        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    return combined_df


def create_spark_session(jars_dir, memory="2g"):
    """Create a Spark session"""
    from pyspark.sql import SparkSession

    spark_jars = [
        jars_dir + "/aws-java-sdk-bundle-1.12.262.jar",
        jars_dir + "/hadoop-aws-3.3.4.jar",
        jars_dir + "/spark-bigquery-with-dependencies_2.12-0.30.0.jar",
        jars_dir + "/gcs-connector-hadoop3-latest.jar",
    ]
    if not check_jars(spark_jars):
        logging.error("JAR files do not exist!")
        return None
    try:
        spark = (
            SparkSession.builder.config("spark.executor.memory", memory)
            .config("spark.jars", ",".join(spark_jars))
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .appName("Batch Processing Application")
            .getOrCreate()
        )
        logging.info("Spark session successfully created!")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def load_minio_config(
    spark_context: SparkContext, minio_access_key, minio_secret_key, minio_endpoint
):
    """Establish the necessary connection to MinIO"""
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


def load_gg_bigquery_config(spark_context: SparkContext, path_to_auth_file):
    """Establish the necessary connection to Google BigQuery"""
    try:
        spark_context._jsc.hadoopConfiguration().set(
            "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        spark_context._jsc.hadoopConfiguration().set(
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        spark_context._jsc.hadoopConfiguration().set(
            "google.cloud.auth.service.account.enable", "true"
        )
        spark_context._jsc.hadoopConfiguration().set(
            "google.cloud.auth.service.account.json.keyfile",
            path_to_auth_file,
        )
        logging.info("Google BigQuery configuration is created successfully")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(
            f"Couldn't create the Google BigQuery configuration due to exception: {e}"
        )


def spark_to_bigquery_type(spark_type):
    """Ánh xạ kiểu dữ liệu từ Spark sang BigQuery."""
    if isinstance(spark_type, TimestampNTZType):
        return "DATETIME"
    elif isinstance(spark_type, StringType):
        return "STRING"
    elif isinstance(spark_type, IntegerType):
        return "INT64"
    elif isinstance(spark_type, LongType):
        return "INT64"
    elif isinstance(spark_type, FloatType):
        return "FLOAT64"
    elif isinstance(spark_type, DoubleType):
        return "FLOAT64"
    elif isinstance(spark_type, BooleanType):
        return "BOOL"
    elif isinstance(spark_type, TimestampType):
        return "TIMESTAMP"
    elif isinstance(spark_type, DateType):
        return "DATE"


def processing_dataframe(df, file_path):
    """Processing the dataframe"""
    from pyspark.sql import functions as F

    df2 = (
        df.withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.date_format("pickup_datetime", "MMM"))
        .withColumn("dow", F.date_format("pickup_datetime", "EEEE"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
    )

    df_final = df2.groupBy(
        "year",
        "month",
        "dow",
        F.col("vendorid").alias("vendor_id"),
        F.col("ratecodeid").alias("rate_code_id"),
        F.col("pulocationid").alias("pickup_location_id"),
        F.col("dolocationid").alias("dropoff_location_id"),
        F.col("payment_type").alias("payment_type_id"),
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
    ).agg(
        F.sum("passenger_count").alias("passenger_count"),
        F.sum("trip_distance").alias("trip_distance"),
        F.sum("extra").alias("extra"),
        F.sum("mta_tax").alias("mta_tax"),
        F.sum("fare_amount").alias("fare_amount"),
        F.sum("tip_amount").alias("tip_amount"),
        F.sum("tolls_amount").alias("tolls_amount"),
        F.sum("total_amount").alias("total_amount"),
        F.sum("improvement_surcharge").alias("improvement_surcharge"),
        F.sum("congestion_surcharge").alias("congestion_surcharge"),
    )

    if "yellow" in file_path:
        df_final = df_final.withColumn("service_type", F.lit(1))
    elif "green" in file_path:
        df_final = df_final.withColumn("service_type", F.lit(2))

    return df_final


def create_bigquery_schema(spark_schema):
    """Tạo schema BigQuery từ Spark schema."""
    bigquery_schema = []
    for field in spark_schema.fields:
        bigquery_schema.append(
            {
                "name": field.name,
                "field_type": spark_to_bigquery_type(field.dataType),
                "mode": "NULLABLE" if field.nullable else "REQUIRED",
            }
        )
    return [bigquery.SchemaField(**field) for field in bigquery_schema]


def create_table_bg(project_id, dataset_id, table_id, df_data, limit=10000):
    """
    Create a table in Google BigQuery
    """
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    spark_schema = df_data.schema
    try:
        # Kiểm tra nếu bảng đã tồn tại
        client.get_table(table_ref)
        print(f"Bảng '{table_id}' đã tồn tại trong dataset '{dataset_id}'.")
    except Exception as e:
        if "Not found" in str(e):
            print(f"Bảng '{table_id}' không tồn tại. Đang tạo bảng mới...")

            # Tạo schema BigQuery từ danh sách dictionary
            bigquery_schema = create_bigquery_schema(spark_schema)
            # Định nghĩa bảng mới
            table = bigquery.Table(table_ref, schema=bigquery_schema)
            # Tạo bảng trong BigQuery
            client.create_table(table)
            print(f"Đã tạo bảng '{table_id}' thành công trong dataset '{dataset_id}'.")
        else:
            raise e
    try:
        df_data.limit(limit).write.format("bigquery").option(
            "table", f"{project_id}:{dataset_id}.{table_id}"
        ).option("writeMethod", "indirect").option(
            "temporaryGcsBucket", "temp_for_bq"
        ).mode(
            "append"
        ).save()

        print(f"Đã ghi dữ liệu thành công vào {table_id}")

    except Exception as e:
        print(f"Lỗi khi ghi dữ liệu{e}")
        raise e

"""
    Converting raw bucket to process bucket.
"""

import os
import logging
from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg
from minio.error import S3Error
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
CFG_FILE = os.path.join(project_root, "config", "datalake.yaml")
cfg_dl = load_cfg(CFG_FILE)["datalake"]

MINIO_ENDPOINT = cfg_dl["endpoint"]
MINIO_ACCESS_KEY = cfg_dl["access_key"]
MINIO_SECRET_KEY = cfg_dl["secret_key"]
BUCKET_NAME_1 = cfg_dl["bucket_name_1"]
BUCKET_NAME_2 = cfg_dl["bucket_name_2"]
BUCKET_NAME_3 = cfg_dl["bucket_name_3"]
YEAR = os.getenv("YEAR_TEST")
MONTH = os.getenv("MONTH_TEST")
DAY = os.getenv("DAY_TEST")

TAXI_LOOKUP_PATH = f"s3a://{BUCKET_NAME_1}/taxi_lookup.csv"


@dataclass
class Config:
    """
    Configuration class
    """

    minio_access_key: str = MINIO_ACCESS_KEY
    minio_secret_key: str = MINIO_SECRET_KEY
    endpoint_url: str = MINIO_ENDPOINT
    jars_dir: str = os.path.join(project_root, "jars")
    taxi_lookup_path: str = TAXI_LOOKUP_PATH
    num_output_files: int = 1
    green_path_src: str = f"s3a://{BUCKET_NAME_1}/cdc_db/{YEAR}/{MONTH}/{DAY}/data"
    yellow_path_src: str = f"s3a://{BUCKET_NAME_1}/yellow/{YEAR}/{MONTH}/{DAY}.parquet"
    green_path_dist: str = f"s3a://{BUCKET_NAME_2}/data/green/{YEAR}/{MONTH}/{DAY}"
    yellow_path_dist: str = f"s3a://{BUCKET_NAME_2}/data/yellow/{YEAR}/{MONTH}/{DAY}"


def create_spark_session(access_key, secret_key, endpoint_url, jars_dir):
    """
    Create a spark session
    """
    from pyspark.sql import SparkSession

    """
        Convert parquet file to delta format
    """
    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip

    jars = [
        jars_dir + "/hadoop-aws-3.3.4.jar",
        jars_dir + "/aws-java-sdk-bundle-1.12.262.jar",
    ]
    # jars = "../../jars/hadoop-aws-3.3.4.jar,../../jars/aws-java-sdk-bundle-1.12.262.jar"

    builder = (
        SparkSession.builder.appName("DeltaConvert")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]
    ).getOrCreate()

    logging.info("Spark session successfully created!")
    return spark


def drop_column(df):
    """
    Drop columns 'store_and_fwd_flag'
    """
    if "store_and_fwd_flag" in df.columns:
        df = df.drop("store_and_fwd_flag")
    return df


def merge_taxi_zone(df, df_lookup):

    def merge_and_rename(df, location_id, lat_col, long_col):
        """
        Merges the DataFrame with lookup data and renames columns.

        Args:
            df (pyspark.sql.dataframe.DataFrame): DataFrame to merge with lookup.
            location_id (str): Column name for location id in the input df
            lat_col (str): New name for latitude column.
            long_col (str): New name for longitude column.

        Returns:
           pyspark.sql.dataframe.DataFrame: Merged and renamed DataFrame.
        """
        df_merged = df.join(
            df_lookup, on=df[location_id] == df_lookup["LocationID"], how="left"
        )
        df_merged = df_merged.drop("LocationID", "Borough", "zone", "service_zone")
        df_merged = df_merged.withColumnRenamed("latitude", lat_col).withColumnRenamed(
            "longitude", long_col
        )
        return df_merged

    if "pickup_latitude" not in df.columns:
        df = merge_and_rename(df, "pulocationid", "pickup_latitude", "pickup_longitude")

    if "dropoff_latitude" not in df.columns:
        df = merge_and_rename(
            df, "dolocationid", "dropoff_latitude", "dropoff_longitude"
        )
    df = df.drop(*[col for col in df.columns if "Unnamed" in col]).dropna()
    return df


def process(df, is_green):
    """
    Green:
        Rename column: lpep_pickup_datetime, lpep_dropoff_datetime
        Drop: trip_type
    Yellow:
        Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
    """
    if is_green:
        # Rename columns
        df = df.withColumnRenamed(
            "lpep_pickup_datetime", "pickup_datetime"
        ).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        # Drop column
        if "trip_type" in df.columns:
            df = df.drop("trip_type")
    else:
        # Rename columns
        df = (
            df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
            .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
            .withColumn("pickup_datetime", col("pickup_datetime").cast("timestamp"))
            .withColumn("dropoff_datetime", col("dropoff_datetime").cast("timestamp"))
            .withColumnRenamed("airport_fee", "fee")
        )

    if "payment_type" in df.columns:
        df = df.fillna(0.0, subset=["payment_type"]).withColumn(
            "payment_type", col("payment_type").cast(IntegerType())
        )
    if "dolocationid" in df.columns:
        df = df.withColumn("dolocationid", col("dolocationid").cast(IntegerType()))
    if "pulocationid" in df.columns:
        df = df.withColumn("pulocationid", col("pulocationid").cast(IntegerType()))
    if "vendorid" in df.columns:
        df = df.withColumn("vendorid", col("vendorid").cast(IntegerType()))

    if "fee" in df.columns:
        df.drop("fee")
    df = df.dropna()

    # Reorder columns alphabetically
    df = df.select(sorted(df.columns))

    return df


def transform_data(df, df_lookup, is_green):
    columns_to_lower = df.columns
    for col_name in columns_to_lower:
        df = df.withColumnRenamed(col_name, col_name.lower())

    df = drop_column(df)
    df = merge_taxi_zone(df, df_lookup)
    df = process(df, is_green)
    return df


def raw_to_processed(args: Config):
    spark = create_spark_session(
        args.minio_access_key,
        args.minio_secret_key,
        args.endpoint_url,
        args.jars_dir,
    )
    taxi_lookup_df = spark.read.csv(args.taxi_lookup_path, header=True)
    logging.info("Taxi lookup data loaded!")
    logging.info(f"Loadi    ng green data... {args.green_path_src}")
    green_df = spark.read.format("delta").load(args.green_path_src)

    print("Records in green before saving: ", green_df.count())

    green_df = transform_data(green_df, taxi_lookup_df, is_green=True)
    green_df = green_df.repartition(args.num_output_files)

    print("Records in green after transform: ", green_df.count())

    green_df.write.format("delta").mode("overwrite").save(args.green_path_dist)

    logging.info("Green data saved!")

    logging.info(f"Loading yellow data... {args.yellow_path_src}")

    yellow_df = spark.read.format("parquet").load(args.yellow_path_src)

    print("Records in yellow before saving: ", yellow_df.count())

    yellow_df = transform_data(yellow_df, taxi_lookup_df, is_green=False)

    print("Records in yellow after transform: ", yellow_df.count())

    yellow_df = yellow_df.repartition(args.num_output_files)

    yellow_df.write.format("delta").mode("overwrite").save(args.yellow_path_dist)

    logging.info("Yellow data saved!")

    print("==============================================")


if __name__ == "__main__":

    args = Config()
    raw_to_processed(args)
# python -m src.batch_processing.raw_to_processed_2

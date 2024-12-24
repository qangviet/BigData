import numpy as np
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from src.utils.minio_utils import MinIOClient
from .upload_to_bigquery import load_data_from_minio
from .upload_to_bigquery import create_table_bg
from .upload_to_bigquery import create_spark_session
from .upload_to_bigquery import load_minio_config
from .upload_to_bigquery import load_cfg
import os
import datetime
import s3fs
import os
import sys
from pyspark.sql.functions import col, to_date,  count, when
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = create_spark_session()
load_minio_config(spark.sparkContext)


project_root  = 'D:/20241/Big_data'

CFG_FILE = os.path.join(project_root, "MyProject/config", "datalake.yaml")
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

BUCKET_NAME_1 = datalake_cfg["bucket_name_1"]
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']


CFG_FILE_BQ = os.path.join(project_root, "MyProject/config", "bigquery.yaml")
bg_cfg = load_cfg(CFG_FILE_BQ)["bigquery"]

BG_PROJECT_ID = bg_cfg["project_id"]
BG_DATASET_ID = bg_cfg["dataset_id"]


MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]



cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

s3_fs = s3fs.S3FileSystem(
    anon=False,
    key=datalake_cfg["access_key"],
    secret=datalake_cfg["secret_key"],
    client_kwargs={"endpoint_url": "".join(["http://", datalake_cfg["endpoint"]])},
)



def save_data(file_path, data):

    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)

    # 1. Gộp dữ liệu về 1 file và ghi vào thư mục tạm
    data.coalesce(1).write.mode("append").parquet(dir_path)

    # 2. Tìm file Parquet trong thư mục tạm
    for file_name in os.listdir(dir_path):
        if file_name.endswith(".parquet") and file_name != 'transform1.parquet':  
            temp_file_path = os.path.join(dir_path, file_name)
            os.rename(temp_file_path, file_path)
            break

    for file_name in os.listdir(dir_path):
        file_path_to_remove = os.path.join(dir_path, file_name)
        if not file_name.endswith(".parquet"): 
            os.remove(file_path_to_remove)

    print(f"File đã được lưu tại: {file_path}")




def get_df_day(date):
    customer_path = f"s3a://{BUCKET_NAME_2}/data_customer.csv"
    df_customer = spark.read.csv(customer_path, header=True, inferSchema=True).select('id_customer')

    taxi_lookup_path = f"s3a://{BUCKET_NAME_2}/taxi_lookup.csv"
    df_taxi_lookup = spark.read.csv(customer_path, header=True, inferSchema=True).select('id_customer')

    df_customer = spark.read.csv(customer_path, header=True, inferSchema=True).select("id_customer")

    # Đọc bảng taxi_lookup
    df_taxi_lookup = spark.read.csv(taxi_lookup_path, header=True, inferSchema=True).select("LocationID")

    # Thực hiện cross join
    df_cross_join  = df_customer.crossJoin(df_taxi_lookup)


    # Lấy dữ liệu chuyến đi từ MinIO
    filtered_data = load_data_from_minio(spark, BUCKET_NAME_2, date, 'Green')

    
    df_pickups = (
    filtered_data
    .groupBy("id_customer", "PULocationID")
    .agg(F.count("*").alias("num_pickups"))
    .withColumnRenamed("PULocationID", "LocationID")
    )

    df_drops = (
        filtered_data
        .groupBy("id_customer", "DOLocationID")
        .agg(F.count("*").alias("num_drops"))
        .withColumnRenamed("DOLocationID", "LocationID")
    )

    # Kết hợp cả pickups và drops vào bảng cross join
    df_joined = (
        df_cross_join
        .join(df_pickups, ["id_customer", "LocationID"], "left")
        .join(df_drops, ["id_customer", "LocationID"], "left")
        .fillna(0, subset=["num_pickups", "num_drops"])  # Thay null bằng 0 nếu không có chuyến xe
    )


    return df_joined


def load_from_transfrom(date):

    year = date.split('-')[0]
    month = date.split('-')[1]
    day = date.split('-')[2]

    file_path = f'D:/20241/Big_data/MyProject/data/transform_data/{year}/{month}/{day}/transform2.parquet'
    df = spark.read.parquet(file_path)

    return df


from datetime import datetime, timedelta

def transform_dynamic(date):

    year = date.split('-')[0]
    month = date.split('-')[1]
    day = date.split('-')[2]

    # Lấy df_current từ ngày cụ thể
    df_current = get_df_day(date)

    # Nếu là ngày 1 tháng 1, lưu ngay df_current
    if month == '01' and day == '01':
        file_path = f"data/transform_data/{year}/{month}/{day}/transform2.parquet"
        save_data(file_path, df_current)
        return df_current

    # Nếu là tháng 1 và không phải ngày 1
    if month == '01':
        previous_day = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
        previous_day_str = previous_day.strftime("%Y-%m-%d")
        df_previous = load_from_transfrom(previous_day_str)

        # Thực hiện join và phép cộng
        combined = df_current.alias("current").join(
            df_previous.alias("previous"), 
            on = ["id_customer", "LocationID"],  
            how="inner"
        ).select(
            F.col("id_customer"),
            F.col("LocationID"),
            (F.coalesce(F.col("current.num_pickups"), F.lit(0)) + 
             F.coalesce(F.col("previous.num_pickups"), F.lit(0))).alias("num_pickups"),
            (F.coalesce(F.col("current.num_drops"), F.lit(0)) + 
             F.coalesce(F.col("previous.num_drops"), F.lit(0))).alias("num_drops"),
        )


        file_path = f"data/transform_data/{year}/{month}/{day}/transform2.parquet"
        save_data(file_path, combined)
        return combined

    # Nếu không phải tháng 1
    else:
        # Lấy dữ liệu 30 ngày trước
        df_30_day_ago = get_df_day((datetime.strptime(date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d"))
        previous_day = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
        previous_day_str = previous_day.strftime("%Y-%m-%d")
        df_previous = load_from_transfrom(previous_day_str)

        # Thực hiện join và phép cộng
        combined = df_current.alias("current").join(
            df_previous.alias("previous"), 
            on = ["id_customer", "LocationID"],  
            how="inner"
        ).select(
            F.col("id_customer"),
            F.col("LocationID"),
            (F.coalesce(F.col("current.num_pickups"), F.lit(0)) + 
             F.coalesce(F.col("previous.num_pickups"), F.lit(0))).alias("num_pickups"),
            (F.coalesce(F.col("current.num_drops"), F.lit(0)) + 
             F.coalesce(F.col("previous.num_drops"), F.lit(0))).alias("num_drops"),
        )


        # Tiếp tục join với dữ liệu 30 ngày trước và thực hiện phép trừ
        result = combined.alias("combined").join(
            df_30_day_ago.alias("thirty_days_ago"), 
            on= ["id_customer", "LocationID"],  
            how="inner"
        ).select(
            F.col("id_customer"),
            F.col("LocationID"),
            (F.coalesce(F.col("combined.num_pickups"), F.lit(0)) - 
             F.coalesce(F.col("thirty_days_ago.num_pickups"), F.lit(0))).alias("num_pickups"),
            (F.coalesce(F.col("combined.num_drops"), F.lit(0)) - 
             F.coalesce(F.col("thirty_days_ago.num_drops"), F.lit(0))).alias("num_drops"),
        )

        file_path = f"data/transform_data/{year}/{month}/{day}/transform2.parquet"
        save_data(file_path, result)
        return result
    


def create_train_data(DATE):
    YEAR = DATE.split('-')[0]
    MONTH = DATE.split('-')[1]
    DAY = DATE.split('-')[2]
    month = int(MONTH) - 1
    previous_month = f"data/transform_data/{YEAR}/{month:02}"

    pre_df = spark.read.format("delta").load(previous_month)
    path_list = []
    combined_df = None

    for i in range(1, int(DAY) + 1):

        # path = f"s3a://{BUCKET_NAME}/{YEAR}/{TAXI_TYPE}/{MONTH}/{i:02}.parquet"
        day = f"{YEAR}-{MONTH}-{i:02}"
        df = load_from_transfrom(day)
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    combined_df = combined_df.union(df)
      



if __name__ == "__main__":
    def process_all_dates(start_date, end_date):


        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        current_date = start_date
        while current_date <= end_date:
        
            date_str = current_date.strftime("%Y-%m-%d")
            
            test = transform_dynamic(date_str)

            current_date += timedelta(days=1)

    start_date = "2024-09-11"
    end_date = "2024-10-31"
    process_all_dates(start_date, end_date)


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
from datetime import datetime, timedelta
from pyspark.sql.functions import col, row_number, first


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


def add_trip_columns(df_current):
    # Cửa sổ để sắp xếp theo id_customer và thời gian pickup
    window_spec = Window.partitionBy("id_customer").orderBy("lpep_pickup_datetime")
    window_spec_desc = Window.partitionBy("id_customer").orderBy(F.desc("lpep_pickup_datetime"))
    
    # Thêm cột num_trips_before
    df_with_trips = df_current.withColumn(
        "num_trips_before",
        F.row_number().over(window_spec) - 1
    )
    
    # Thêm cột is_last_trip
    df_with_last_trip = df_with_trips.withColumn(
        "is_last_trip",
        F.when(F.row_number().over(window_spec_desc) == 1, 1).otherwise(0)
    )
    
    return df_with_last_trip.select('id_customer', 'num_trips_before', 'is_last_trip')



def get_train_data(date):
    # Lấy ngày trước đó
    previous_day = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)    
    previous_day_str = previous_day.strftime("%Y-%m-%d")

    year = previous_day_str.split('-')[0]
    month = previous_day_str.split('-')[1]
    day = previous_day_str.split('-')[2]

    # Đường dẫn các file transform
    file_path_1  = f"data/transform_data/{year}/{month}/{day}/transform1.parquet"
    file_path_2  = f"data/transform_data/{year}/{month}/{day}/transform2.parquet"

    # Load dữ liệu
    df_current = load_data_from_minio(spark, BUCKET_NAME_2, date, 'Green')
    df_transform1 = spark.read.parquet(file_path_1)
    df_transform2 = spark.read.parquet(file_path_2)

    # Chọn các cột cần thiết từ df_current
    df_selected = df_current.select(
        col("id").alias("trip_id"),
        col("id_customer"),
        col("lpep_pickup_datetime").alias("pickup_time"),
        col("PULocationID").alias("pickup_location"),
        col("DOLocationID").alias("drop_location")
    )

    # Join với df_transform1
    df_result = df_selected.join(
        df_transform1,
        on="id_customer",
        how="inner"
    ).select(
        "trip_id", "id_customer", "pickup_time", "pickup_location", "drop_location", "count_lager_2_trips"
    )

    # Tìm top 1 và top 2 pickup_location
    pickup_window = Window.partitionBy("id_customer").orderBy(col("num_pickups").desc())
    top_pickups = df_transform2.withColumn("pickup_rank", row_number().over(pickup_window)) \
        .filter(col("pickup_rank") <= 2) \
        .select("id_customer", "LocationID", "pickup_rank")

    top_pickups_pivot = top_pickups.groupBy("id_customer").pivot("pickup_rank", [1, 2]) \
        .agg(first("LocationID").alias("pickup_location"))

    top_pickups_pivot = top_pickups_pivot \
        .withColumnRenamed("1", "top1_pickup_location") \
        .withColumnRenamed("2", "top2_pickup_location")

    # Tìm top 1 và top 2 drop_location
    drop_window = Window.partitionBy("id_customer").orderBy(col("num_drops").desc())
    top_drops = df_transform2.withColumn("drop_rank", row_number().over(drop_window)) \
        .filter(col("drop_rank") <= 2) \
        .select("id_customer", "LocationID", "drop_rank")

    top_drops_pivot = top_drops.groupBy("id_customer").pivot("drop_rank", [1, 2]) \
        .agg(first("LocationID").alias("drop_location"))

    top_drops_pivot = top_drops_pivot \
        .withColumnRenamed("1", "top1_drop_location") \
        .withColumnRenamed("2", "top2_drop_location")

    # Gộp thông tin top locations vào bảng result
    result_with_top_locations = df_result \
        .join(top_pickups_pivot, on="id_customer", how="left") \
        .join(top_drops_pivot, on="id_customer", how="left")


    result_with_top_locations.show(5)
    result_with_top_locations.printSchema()
    df_with_last_trip = add_trip_columns(df_current)
    result = result_with_top_locations.join(df_with_last_trip, on="id_customer", how="inner")


    result.printSchema()
    result.show(30)
    return result



def aggregate_results_from_date(date: str):
   
    current_date = datetime.strptime(date, "%Y-%m-%d")

    first_day_of_last_month = (current_date.replace(day=1) - timedelta(days=1)).replace(day=1)

    dates_to_process = [(first_day_of_last_month + timedelta(days=i)).strftime("%Y-%m-%d") 
                        for i in range((current_date - first_day_of_last_month).days + 1)]

    aggregated_result = None

    # Lặp qua từng ngày
    for process_date in dates_to_process:
        print(f"Processing date: {process_date}")
        daily_result = get_train_data(process_date)  # Gọi hàm get_train_data cho từng ngày

        # Nối các kết quả
        if aggregated_result is None:
            aggregated_result = daily_result
        else:
            aggregated_result = aggregated_result.union(daily_result)

    # Trả về DataFrame tổng hợp
    return aggregated_result


if __name__ == "__main__":
    get_train_data('2024-01-02')
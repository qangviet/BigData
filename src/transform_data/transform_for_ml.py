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




def analyze_top_customers(df, k, df_taxi_lookup, num_customer):
    """
    Phân tích top k người dùng hay đi xe nhất, bao gồm:
    - Ngày phổ biến nhất
    - Buổi trong ngày phổ biến nhất
    - Địa điểm đón phổ biến nhất
    - Địa điểm trả phổ biến nhất

    Args:
    - df (DataFrame): DataFrame dữ liệu taxi.
    - k (int): Số lượng người dùng hàng đầu cần phân tích.

    Returns:
    - DataFrame: DataFrame chứa thông tin phân tích.
    """
    
    # Kiểm tra và thêm cột customerID nếu chưa tồn tại
    if 'customerID' not in df.columns:
        df = df.withColumn("customerID", (F.rand(seed=42) * num_customer).cast("int") + 1)
    
    # Thêm thông tin thời gian
    df = df.withColumn("pickup_hour", F.hour("pickup_datetime")) \
           .withColumn("pickup_day", F.date_format("pickup_datetime", "EEEE")) \
           .withColumn("pickup_time_of_day", 
                       F.when((F.col("pickup_hour") >= 5) & (F.col("pickup_hour") < 12), "Sáng")
                        .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 18), "Chiều")
                        .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") < 23), "Tối")
                        .otherwise("Đêm"))
    
    # Đếm số chuyến đi của từng người dùng
    customer_trip_count = df.groupBy("customerID").count().withColumnRenamed("count", "trip_count")
    
    # Lấy top k người dùng
    top_customers = customer_trip_count.orderBy(F.desc("trip_count")).limit(k)
    
    top_customers_data = df.join(top_customers, "customerID", "inner")
    
    # Tính các giá trị phổ biến nhất (mode) - sử dụng groupBy và agg
    result = top_customers_data.groupBy("customerID", "pickup_day", "pickup_time_of_day", "pulocationid") \
        .agg(F.count("*").alias("count")) \
        .withColumn("rank", F.row_number().over(Window.partitionBy("customerID").orderBy(F.desc("count"))))
    
    # Lọc ra giá trị phổ biến nhất cho từng người dùng
    most_common_info = result.filter(F.col("rank") == 1).drop("rank")
    
    # Thêm thông tin về địa điểm từ df_taxi_lookup
    result2 = (
        most_common_info
        .join(df_taxi_lookup, most_common_info.pulocationid == df_taxi_lookup.LocationID, "inner")
        .select(
            most_common_info.customerID,
            most_common_info.pickup_day,
            most_common_info.pickup_time_of_day,
            most_common_info.pulocationid.alias("LocationID"),
            df_taxi_lookup.zone,
            df_taxi_lookup.service_zone
        )
    )

    return result2


#  Tìm số chuyến xe đi, tổng số tiền (fare amount), địa điểm hay đi(locationID) thời gian đi(buổi sang, trưa, tối) của một CustomerID
def analyze_customer_info(df, customer_id, df_taxi_lookup, num_customer):
    # Lọc dữ liệu của customer_id

    if 'customerID' not in df.columns:
        df = df.withColumn("customerID", (F.rand(seed=42) * num_customer).cast("int") + 1)

    df_customer = df.filter(F.col("customerID") == customer_id)

    # Thêm thông tin thời gian
    df_customer = df_customer.withColumn("pickup_hour", F.hour("pickup_datetime")) \
           .withColumn("pickup_day", F.date_format("pickup_datetime", "EEEE")) \
           .withColumn("pickup_time_of_day", 
                       F.when((F.col("pickup_hour") >= 5) & (F.col("pickup_hour") < 12), "Sáng")
                        .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 18), "Chiều")
                        .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") < 23), "Tối")
                        .otherwise("Đêm"))

    # Tính tổng hợp thông tin của customer
    customer_summary = df_customer.agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_spent"),
        F.avg("total_amount").alias("avg_spent"),
        F.sum("tip_amount").alias("total_tip"),
        F.avg("tip_amount").alias("avg_tip"),
    ).collect()[0]  # Collect để lấy giá trị tổng hợp từ customer_summary

    # Tính địa điểm hay đi nhất
    location_count = df_customer.groupBy("pulocationid").agg(
        F.count("*").alias("count")
    ).withColumn("rank", 
        
        F.row_number().over(Window.orderBy(F.desc("count")))
        
        )

    # Lọc địa điểm hay đi nhất
    most_common_location = location_count.filter(F.col("rank") == 1).drop("rank")
  

    # Tính buổi trong ngày hay đi nhất
    time_of_day_count = df_customer.groupBy("pickup_time_of_day").agg(
        F.count("*").alias("count")
    ).withColumn("rank", 
                 
        F.row_number().over(Window.orderBy(F.desc("count")))
        
        )


    # Lọc buổi trong ngày hay đi nhất
    most_common_time_of_day = time_of_day_count.filter(F.col("rank") == 1).drop("rank")

  
    # Kết hợp thông tin từ customer_summary vào most_common_location
    most_common_location = most_common_location.withColumn(
        "total_spent", F.lit(customer_summary["total_spent"])
    ).withColumn(
        "avg_spent", F.lit(customer_summary["avg_spent"])
    ).withColumn(
        "total_tip", F.lit(customer_summary["total_tip"])
    ).withColumn(
        "avg_tip", F.lit(customer_summary["avg_tip"])
    ).withColumn(
        "customerID", F.lit(customer_id)  # Thêm cột customerID
    ).withColumn(
        "pickup_time_of_day", F.lit(most_common_time_of_day.collect()[0]["pickup_time_of_day"]) 
    )


    # Kết hợp với thông tin về zone từ df_taxi_lookup (nối ngang)
    result = most_common_location.join(df_taxi_lookup, df_taxi_lookup.LocationID == most_common_location.pulocationid, "inner")\
    .select(
        "customerID",
        "count",
        "total_spent",
        "avg_spent",
        "total_tip",
        "avg_tip",
        "LocationID",
        "zone",
        "service_zone",
        "pickup_time_of_day",
    )
    return result




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





def get_df_day(date):
 
       # Đọc dữ liệu khách hàng từ file CSV
    path = f"s3a://{BUCKET_NAME_2}/data_customer.csv"
    df_customer = spark.read.csv(path, header=True, inferSchema=True).select('id_customer')


    # Lấy dữ liệu chuyến đi từ MinIO
    filtered_data = load_data_from_minio(spark, BUCKET_NAME_2, date, 'Green')

    # Tính số chuyến đi theo mỗi khách hàng
    trip_counts = (
        filtered_data.groupBy("id_customer")
        .agg(count("*").alias("trip_count"))
    )

    # Tạo cột 'count_lager_2_trips' với giá trị 1 nếu số chuyến đi > 2, nếu không thì là 0
    trip_counts = trip_counts.withColumn(
        "count_lager_2_trips", when(col("trip_count") > 2, 1).otherwise(0)
    )

    # Left join với df_customer để đảm bảo tất cả khách hàng có trong kết quả
    result = df_customer.join(trip_counts, on="id_customer", how="left")

    # Nếu một khách hàng không có chuyến đi trong ngày (null trong 'trip_count'), gán 'count_lager_2_trips' là 0
    result = result.fillna({"count_lager_2_trips": 0})

    # Trả về kết quả với cột 'id_customer' và 'count_lager_2_trips'
    return result.select("id_customer", "count_lager_2_trips")


def load_from_transfrom(date):

    year = date.split('-')[0]
    month = date.split('-')[1]
    day = date.split('-')[2]

    file_path = f'D:/20241/Big_data/MyProject/data/transform_data/{year}/{month}/{day}/transform1.parquet'
    df = spark.read.parquet(file_path)

    return df

from datetime import datetime, timedelta


def transform_dynamic(date):
    # Tạo SparkSession nếu chưa có
    spark = SparkSession.builder.appName("GetCustomerTrips").getOrCreate()
    load_minio_config(spark.sparkContext)

    # Tách năm, tháng, ngày từ đầu vào
    year = date.split('-')[0]
    month = date.split('-')[1]
    day = date.split('-')[2]

    # Lấy df_current từ ngày cụ thể
    df_current = get_df_day(date)

    # Nếu là ngày 1 tháng 1, lưu ngay df_current
    if month == '01' and day == '01':
        file_path = f"data/transform_data/{year}/{month}/{day}/transform1.parquet"
        df_current.coalesce(1).write.option("spark.sql.parquet.writeLegacyFormat", "true") \
            .option("spark.sql.parquet.compression.codec", "none") \
            .mode("overwrite").parquet(file_path)
        print(f"File đã được lưu tại: {file_path}")
        return df_current

    # Nếu là tháng 1 và không phải ngày 1
    if month == '01':
        previous_day = datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
        previous_day_str = previous_day.strftime("%Y-%m-%d")
        df_previous = load_from_transfrom(previous_day_str)

        # Thực hiện join và phép cộng
        combined = df_current.alias("current").join(
            df_previous.alias("previous"), 
            on="id_customer", 
            how="outer"
        ).select(
            F.col("id_customer"),
            (F.coalesce(F.col("current.count_lager_2_trips"), F.lit(0)) + 
             F.coalesce(F.col("previous.count_lager_2_trips"), F.lit(0))).alias("count_lager_2_trips")
        )
        
        file_path = f"data/transform_data/{year}/{month}/{day}/transform1.parquet"
        combined.coalesce(1).write.option("spark.sql.parquet.writeLegacyFormat", "true") \
            .option("spark.sql.parquet.compression.codec", "none") \
            .mode("overwrite").parquet(file_path)
        print(f"File đã được lưu tại: {file_path}")
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
            on="id_customer", 
            how="outer"
        ).select(
            F.col("id_customer"),
            (F.coalesce(F.col("current.count_lager_2_trips"), F.lit(0)) + 
             F.coalesce(F.col("previous.count_lager_2_trips"), F.lit(0))).alias("count_lager_2_trips")
        )

        # Tiếp tục join với dữ liệu 30 ngày trước và thực hiện phép trừ
        result = combined.alias("combined").join(
            df_30_day_ago.alias("thirty_days_ago"), 
            on="id_customer", 
            how="left"
        ).select(
            F.col("id_customer"),
            (F.coalesce(F.col("combined.count_lager_2_trips"), F.lit(0)) - 
             F.coalesce(F.col("thirty_days_ago.count_lager_2_trips"), F.lit(0))).alias("count_lager_2_trips")
        )

        file_path = f"data/transform_data/{year}/{month}/{day}/transform1.parquet"
        result.coalesce(1).write.mode("overwrite").parquet(file_path)
        print(f"File đã được lưu tại: {file_path}")
        return result



if __name__ == "__main__":
    def process_all_dates(start_date, end_date):


        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        current_date = start_date
        while current_date <= end_date:
        
            date_str = current_date.strftime("%Y-%m-%d")
            
            test = transform_dynamic(date_str)

            test.show()

            current_date += timedelta(days=1)

    start_date = "2024-01-01"
    end_date = "2024-01-01"
    process_all_dates(start_date, end_date)

    test_df = get_df_day( "2024-01-01")

    test_df.show()

    df = test_df.toPandas()


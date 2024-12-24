from pyspark.sql.window import Window
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, row_number, max as spark_max, when
from batch_processing.upload_to_bigquery import (
    create_table_bg,
    load_minio_config,
    create_spark_session,
    load_cfg,
    load_gg_bigquery_config,
    load_data_from_minio_for_visualize,
    load_data_from_minio,
)
import os
from dotenv import load_dotenv

load_dotenv()
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)


def calculate_per_day(spark, date, taxi_type, bucket_name):

    day_date = F.lit(date).cast(DateType())
    start_of_month = F.trunc(day_date, "MM")

    # df_data là dữ liệu từ đầu tháng đến ngày hiện tại
    df_data = load_data_from_minio_for_visualize(spark, bucket_name, date, taxi_type)

    # df_filtered là dữ liệu chỉ cho ngày hiện tại
    df_filtered = load_data_from_minio(spark, bucket_name, date, taxi_type)

    # Tính tổng doanh thu theo ngày
    df_revenue = df_filtered.groupBy("pickup_date").agg(
        F.sum("total_amount").alias("revenue_of_day")
    )

    # Tính trung bình tiền boa
    df_avg_amount = df_filtered.groupBy("pickup_date").agg(
        F.avg("tip_amount").alias("avg_tip_amount")
    )

    # Tính tổng fare_amount
    df_total_fare = df_filtered.groupBy("pickup_date").agg(
        F.sum("fare_amount").alias("total_fare")
    )

    # Tính tổng khoảng cách
    df_total_distance = df_filtered.groupBy("pickup_date").agg(
        F.sum("trip_distance").alias("total_trip_distance")
    )

    # Tính tổng khách hàng
    df_total_customer = df_filtered.groupBy("pickup_date").agg(
        F.sum("passenger_count").alias("total_customer")
    )

    # Tính thời gian trung bình của mỗi chuyến
    df_avg_trip_time = (
        df_filtered.withColumn(
            "trip_duration",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
            / 60,  # Tính thời gian chuyến đi (phút)
        )
        .groupBy("pickup_date")
        .agg(F.avg("trip_duration").alias("avg_trip_time"))
    )

    # Tính doanh thu từ đầu tháng đến ngày hiện tại (sử dụng df_data)
    df_revenue_from_start_of_month = df_data.agg(
        F.sum("total_amount").alias("revenue_of_month")
    )

    # Kết hợp tất cả các kết quả lại với nhau
    df_final = (
        df_revenue.join(df_avg_amount, on="pickup_date", how="left")
        .join(df_total_fare, on="pickup_date", how="left")
        .join(df_total_distance, on="pickup_date", how="left")
        .join(df_avg_trip_time, on="pickup_date", how="left")
        .join(df_revenue_from_start_of_month, on=None, how="left")
        .join(df_total_customer, on="pickup_date", how="left")
        .withColumn("date", day_date)
    )

    # Chọn ra các cột cần thiết
    df_final = df_final.select(
        "date",
        "revenue_of_day",
        "revenue_of_month",
        "avg_tip_amount",
        "total_fare",
        "total_trip_distance",
        "avg_trip_time",
        "total_customer",
    )

    # Chỉ lấy một hàng
    df_final = df_final.limit(1)

    return df_final


def calculate_per_day_for_location(spark, df_taxi_lookup, date, taxi_type, bucket_name):
    # Chuyển DAY sang DateType
    day_date = F.lit(date).cast(DateType())

    # Lọc dữ liệu theo ngày
    df_filtered = load_data_from_minio(spark, bucket_name, date, taxi_type)

    # Nhóm theo pulocationid và tính toán các thông số
    df_grouped = df_filtered.groupBy("pulocationid").agg(
        # Tính tổng doanh thu
        F.sum("total_amount").alias("total_amount"),
        # Tính trung bình tiền boa
        F.avg("tip_amount").alias("avg_tip_amount"),
        # Tính tổng fare_amount
        F.sum("fare_amount").alias("total_fare"),
        # Tính trung bình khoảng cách
        F.avg("trip_distance").alias("avg_trip_distance"),
        # Tính trung bình tolls_amount
        F.avg("tolls_amount").alias("avg_tolls_amount"),
        # Tính trung bình congestion surcharge
        F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
        # Tính tổng số chuyến
        F.count("pulocationid").alias("total_trip_count"),
    )

    # Kết hợp cột "date" vào để có thông tin về ngày
    df_final = df_grouped.withColumn("date", day_date)

    # Thực hiện join với bảng taxi_lookup để thêm thông tin zone
    df_with_zone = df_final.join(
        df_taxi_lookup,
        df_grouped["pulocationid"] == df_taxi_lookup["LocationID"],
        "left",
    ).select(
        "date",
        "pulocationid",
        "zone",
        "total_amount",
        "avg_tip_amount",
        "total_fare",
        "avg_trip_distance",
        "avg_tolls_amount",
        "avg_congestion_surcharge",
        "total_trip_count",
    )

    return df_with_zone


# Lấy top k người đi xe nhiều nhất và cùng với thời gian địa điểm
def analyze_top_customers(df, k, df_taxi_lookup, num_customer):
    """
    Phân tích top k người dùng hay đi xe nhất, bao gồm:
    - Ngày phổ biến nhất
    - Buổi trong ngày phổ biến nhất
    - Địa điểm đón phổ biến nhất
    - Địa điểm trả phổ biến nhất
    that khong

    Args:
    - df (DataFrame): DataFrame dữ liệu taxi.
    - k (int): Số lượng người dùng hàng đầu cần phân tích.

    Returns:
    - DataFrame: DataFrame chứa thông tin phân tích.
    """

    # Kiểm tra và thêm cột customerID nếu chưa tồn tại
    if "customerID" not in df.columns:
        df = df.withColumn(
            "customerID", (F.rand(seed=42) * num_customer).cast("int") + 1
        )

    # Thêm thông tin thời gian
    df = (
        df.withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_day", F.date_format("pickup_datetime", "EEEE"))
        .withColumn(
            "pickup_time_of_day",
            F.when((F.col("pickup_hour") >= 5) & (F.col("pickup_hour") < 12), "Sáng")
            .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 18), "Chiều")
            .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") < 23), "Tối")
            .otherwise("Đêm"),
        )
    )

    # Đếm số chuyến đi của từng người dùng
    customer_trip_count = (
        df.groupBy("customerID").count().withColumnRenamed("count", "trip_count")
    )

    # Lấy top k người dùng
    top_customers = customer_trip_count.orderBy(F.desc("trip_count")).limit(k)

    top_customers_data = df.join(top_customers, "customerID", "inner")

    # Tính các giá trị phổ biến nhất (mode) - sử dụng groupBy và agg
    result = (
        top_customers_data.groupBy(
            "customerID", "pickup_day", "pickup_time_of_day", "pulocationid"
        )
        .agg(F.count("*").alias("count"))
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("customerID").orderBy(F.desc("count"))
            ),
        )
    )

    # Lọc ra giá trị phổ biến nhất cho từng người dùng
    most_common_info = result.filter(F.col("rank") == 1).drop("rank")

    # Thêm thông tin về địa điểm từ df_taxi_lookup
    result2 = most_common_info.join(
        df_taxi_lookup,
        most_common_info.pulocationid == df_taxi_lookup.LocationID,
        "inner",
    ).select(
        most_common_info.customerID,
        most_common_info.pickup_day,
        most_common_info.pickup_time_of_day,
        most_common_info.pulocationid.alias("LocationID"),
        df_taxi_lookup.zone,
        df_taxi_lookup.service_zone,
    )

    return result2


#  Tìm số chuyến xe đi, tổng số tiền (fare amount), địa điểm hay đi(locationID) thời gian đi(buổi sang, trưa, tối) của một CustomerID
def analyze_customer_info(df, customer_id, df_taxi_lookup, num_customer):
    # Lọc dữ liệu của customer_id

    if "customerID" not in df.columns:
        df = df.withColumn(
            "customerID", (F.rand(seed=42) * num_customer).cast("int") + 1
        )

    df_customer = df.filter(F.col("customerID") == customer_id)

    # Thêm thông tin thời gian
    df_customer = (
        df_customer.withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_day", F.date_format("pickup_datetime", "EEEE"))
        .withColumn(
            "pickup_time_of_day",
            F.when((F.col("pickup_hour") >= 5) & (F.col("pickup_hour") < 12), "Sáng")
            .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 18), "Chiều")
            .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") < 23), "Tối")
            .otherwise("Đêm"),
        )
    )

    # Tính tổng hợp thông tin của customer
    customer_summary = df_customer.agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_spent"),
        F.avg("total_amount").alias("avg_spent"),
        F.sum("tip_amount").alias("total_tip"),
        F.avg("tip_amount").alias("avg_tip"),
    ).collect()[
        0
    ]  # Collect để lấy giá trị tổng hợp từ customer_summary

    # Tính địa điểm hay đi nhất
    location_count = (
        df_customer.groupBy("pulocationid")
        .agg(F.count("*").alias("count"))
        .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("count"))))
    )

    # Lọc địa điểm hay đi nhất
    most_common_location = location_count.filter(F.col("rank") == 1).drop("rank")

    # Tính buổi trong ngày hay đi nhất
    time_of_day_count = (
        df_customer.groupBy("pickup_time_of_day")
        .agg(F.count("*").alias("count"))
        .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("count"))))
    )

    # Lọc buổi trong ngày hay đi nhất
    most_common_time_of_day = time_of_day_count.filter(F.col("rank") == 1).drop("rank")

    # Kết hợp thông tin từ customer_summary vào most_common_location
    most_common_location = (
        most_common_location.withColumn(
            "total_spent", F.lit(customer_summary["total_spent"])
        )
        .withColumn("avg_spent", F.lit(customer_summary["avg_spent"]))
        .withColumn("total_tip", F.lit(customer_summary["total_tip"]))
        .withColumn("avg_tip", F.lit(customer_summary["avg_tip"]))
        .withColumn("customerID", F.lit(customer_id))  # Thêm cột customerID
        .withColumn(
            "pickup_time_of_day",
            F.lit(most_common_time_of_day.collect()[0]["pickup_time_of_day"]),
        )
    )

    # Kết hợp với thông tin về zone từ df_taxi_lookup (nối ngang)
    result = most_common_location.join(
        df_taxi_lookup,
        df_taxi_lookup.LocationID == most_common_location.pulocationid,
        "inner",
    ).select(
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


project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

CFG_FILE = os.path.join(project_root, "config", "datalake.yaml")
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

BUCKET_NAME_1 = datalake_cfg["bucket_name_1"]
BUCKET_NAME_2 = datalake_cfg["bucket_name_2"]


CFG_FILE_BQ = os.path.join(project_root, "config", "bigquery.yaml")
bg_cfg = load_cfg(CFG_FILE_BQ)["bigquery"]

BQ_PROJECT_ID = bg_cfg["project_id"]
BQ_DATASET_ID = bg_cfg["dataset_id"]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY")
YEAR = os.getenv("YEAR_TEST")
MONTH = os.getenv("MONTH_TEST")
DAY = os.getenv("DAY_TEST")


@dataclass
class Config:
    """
    Configuration class
    """

    max_memory: str = "4g"
    minio_endpoint: str = MINIO_ENDPOINT
    minio_access_key: str = MINIO_ACCESS_KEY
    minio_secret_key: str = MINIO_SECRET
    jars_dir: str = os.path.join(project_root, "jars")
    bucket_name: str = BUCKET_NAME_2
    year: str = YEAR
    month: str = MONTH
    day: str = DAY
    date: str = f"{YEAR}-{MONTH}-{DAY}"
    taxi_lookup_path: str = f"s3a://{BUCKET_NAME_1}/taxi_lookup.csv"
    bq_project_id: str = BQ_PROJECT_ID
    bq_dataset_id: str = BQ_DATASET_ID
    path_auth_file: str = "E:/BigData_2/authencation/account_key_gg_bigquery.json"


def load_transform_save(args: Config):
    spark = create_spark_session(args.jars_dir, args.max_memory)
    load_gg_bigquery_config(spark.sparkContext, args.path_auth_file)
    load_minio_config(
        spark.sparkContext,
        args.minio_access_key,
        args.minio_secret_key,
        args.minio_endpoint,
    )
    df_taxi_lookup = spark.read.csv(
        args.taxi_lookup_path, header=True, inferSchema=True
    )
    logging.info("Done load taxi lookup")
    df_day_yellow = calculate_per_day(spark, args.date, "yellow", args.bucket_name)

    create_table_bg(
        args.bq_project_id,
        args.bq_dataset_id,
        f"YELLOW_DAILY_MONTH_TABLE_{args.year}_{args.month}",
        df_day_yellow,
    )
    logging.info("Done yellow daily table")
    df_day_location_yellow = calculate_per_day_for_location(
        spark, df_taxi_lookup, args.date, "yellow", args.bucket_name
    )
    create_table_bg(
        args.bq_project_id,
        args.bq_dataset_id,
        f"YELLOW_DAILY_TABLE_{args.year}_{args.month}_{args.day}",
        df_day_location_yellow,
    )
    logging.info("Done yellow daily table 2")

    df_day_green = calculate_per_day(spark, args.date, "green", args.bucket_name)

    create_table_bg(
        args.bq_project_id,
        args.bq_dataset_id,
        f"GREEN_DAILY_MONTH_TABLE_{args.year}_{args.month}",
        df_day_green,
    )
    logging.info("Done green daily table")
    df_day_location_green = calculate_per_day_for_location(
        spark, df_taxi_lookup, args.date, "green", args.bucket_name
    )
    create_table_bg(
        args.bq_project_id,
        args.bq_dataset_id,
        f"GREEN_DAILY_TABLE_{args.year}_{args.month}_{args.day}",
        df_day_location_green,
    )
    logging.info("Done green daily table 2")
    print("Quá trình xử lý hoàn tất!")


if __name__ == "__main__":
    args = Config()
    load_transform_save(args)
    # print(args)

# python ./src/batch_processing/load_and_transform.py

"""
    Converting raw bucket to process bucket.
"""

import os
import pandas as pd
from glob import glob

from utils.helpers import load_cfg
from utils.minio_utils import MinIOClient

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
YEARS = ["2023", "2024"]
MONTH = "01"
TAXI_LOOKUP_PATH = os.path.join(project_root, "data", "taxi_lookup.csv")
CFG_FILE = os.path.join(project_root, "config", "datalake.yaml")
DATA_PATH = os.path.join(project_root, "data/new")


def drop_column(df, file):
    """
    Drop columns 'store_and_fwd_flag'
    """
    if "store_and_fwd_flag" in df.columns:
        df.drop(columns=["store_and_fwd_flag"])
        print("Dropped column store_and_fwd_flag from file: " + file)
    else:
        print("Column store_and_fwd_flag not found in file: " + file)

    return df


def merge_taxi_zone(df, file):
    """
    Merge dataset with taxi zone lookup
    """
    df_lookup = pd.read_csv(TAXI_LOOKUP_PATH)

    def merge_and_rename(df, location_id, lat_col, long_col):
        df = df.merge(df_lookup, left_on=location_id, right_on="LocationID")
        df = df.drop(columns=["LocationID", "Borough", "zone", "service_zone"])
        df = df.rename(columns={"latitude": lat_col, "longitude": long_col})
        return df

    if "pickup_latitude" not in df.columns:
        df = merge_and_rename(df, "pulocationid", "pickup_latitude", "pickup_longitude")
    if "dropoff_latitude" not in df.columns:
        df = merge_and_rename(
            df, "dolocationid", "dropoff_latitude", "dropoff_longitude"
        )

    df = df.drop(
        columns=[col for col in df.columns if "Unnamed" in col], errors="ignore"
    ).dropna()

    print("Merged file: " + file)

    return df


def process(df, file):
    """
    Green:
        Rename column: lpep_pickup_datetime, lpep_dropoff_datetime
        Drop: trip_type
    Yellow:
        Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
    """

    if file.startswith("green"):
        # Rename columns
        df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
            },
            inplace=True,
        )

        # Drop columns
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)

    elif file.startswith("yellow"):
        # Rename columns
        df.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "airport_fee": "fee",
            },
            inplace=True,
        )

    # fix data type in colums "payment_type", "dolocationid",
    # "pulocationid", "vendorid" to int
    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].fillna(0.0).astype(int)
    if "dolocationid" in df.columns:
        df["dolocationid"] = df["dolocationid"].astype(int)
    if "pulocationid" in df.columns:
        df["pulocationid"] = df["pulocationid"].astype(int)
    if "vendorid" in df.columns:
        df["vendorid"] = df["vendorid"].astype(int)

    # drop column "fee"

    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)

    # Remove missing data
    df = df.dropna()
    df = df.reindex(sorted(df.columns), axis=1)

    print("Transformed file: " + file)

    return df


def transform_data():
    """
    Transform data after loading into Datalake (MinIO)
    """
    import s3fs

    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    s3_fs = s3fs.S3FileSystem(
        anon=False,
        key=datalake_cfg["access_key"],
        secret=datalake_cfg["secret_key"],
        client_kwargs={"endpoint_url": "".join(["http://", datalake_cfg["endpoint"]])},
    )

    client = MinIOClient(
        datalake_cfg["endpoint"], datalake_cfg["access_key"], datalake_cfg["secret_key"]
    )

    client.create_bucket(datalake_cfg["bucket_name_2"])

    for year in YEARS:
        all_fps = glob(os.path.join(DATA_PATH, year, "*.parquet"))
        print(all_fps)
        for file in all_fps:

            file_name = os.path.basename(file)
            print(f"Reading parquet file: {file_name}")

            df = pd.read_parquet(file, engine="pyarrow")

            df.columns = df.columns.str.lower()
            print("Ori:", len(df))
            df = drop_column(df, file_name)
            df = merge_taxi_zone(df, file_name)
            df = process(df, file_name)
            print("After:", len(df))
            path = f"s3://{datalake_cfg['bucket_name_2']}/{year}/{file_name}"
            df.to_parquet(path, index=False, filesystem=s3_fs, engine="pyarrow")
            print("Finished transforming data in file: " + path)
            print("=" * 100)

if __name__ == "__main__":
    transform_data()

# py -m src.batch_processing.raw_to_processed

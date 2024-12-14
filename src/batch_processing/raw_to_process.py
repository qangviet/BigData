"""
    Converting raw bucket to process bucket.
"""

import os
import pandas as pd
from glob import glob

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
YEARS = ["2024"]
TAXI_LOOKUP_PATH = os.path.join(project_root, "data", "taxi_lookup.csv")
CFG_FILE = os.path.join(project_root, "config", "datalake.yaml")


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
        df = df.drop(columns=["LocationID", "Borough", "Zone", "service_zone"])
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

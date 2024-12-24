"""
    Upload from local to raw bucket in MinIO.
"""

import sys
import os
from glob import glob
#import
#Upload
from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
CFG_FILE = os.path.join(project_root, "config", "datalake.yaml")

YEAR = "2024"
MONTHS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
DAYS = [str(i).zfill(2) for i in range(1, 32)]

DATA_DIR = os.path.join(project_root, "data/new_2")


def extract_load(cfg):
    datalake_cfg = cfg["datalake"]

    # Create MinIO client
    client = MinIOClient(
        endpoint_url=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
    )

    client.create_bucket(datalake_cfg["bucket_name_1"])

    # for year in YEARS:
    # ...
    # Upload files
    client_minio = client.create_conn()
    for month in MONTHS:
        for day in DAYS:
            file_path = os.path.join(DATA_DIR, f"{YEAR}/Yellow/{month}/{day}.parquet")
            if os.path.exists(file_path):
                print(f"Uploading {file_path} to MinIO...")
                client_minio.fput_object(
                    bucket_name=datalake_cfg["bucket_name_1"],
                    object_name=f"yellow/{YEAR}/{month}/{day}.parquet",
                    file_path=file_path,
                )


if __name__ == "__main__":
    print("Extracting and loading data to MinIO...")
    cfg = load_cfg(CFG_FILE)
    extract_load(cfg)

# py -m src.batch_processing.upload_from_local

"""
    Upload taxi lookup file from local to process bucket in MinIO.
"""

import sys
import os
from glob import glob


from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg


project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
CFG_FILE = os.path.join(project_root, "config", "datalake.yaml")


NYC_DATA_DIR = os.path.join(project_root, "data")


def extract_load(cfg):
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    # Create MinIO client
    client = MinIOClient(
        endpoint_url=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
    )

    # client.create_bucket(datalake_cfg["bucket_name_1"])


    fp = os.path.join(NYC_DATA_DIR, 'taxi_lookup.csv')
    print(f"Uploading {fp} to MinIO...")
    client_minio = client.create_conn()
    client_minio.fput_object(
        bucket_name=datalake_cfg["bucket_name_2"],
        object_name=os.path.basename(fp),
        file_path=fp,
    )


if __name__ == "__main__":
    print("Extracting and loading data to MinIO...")
    cfg = load_cfg(CFG_FILE)
    extract_load(cfg)

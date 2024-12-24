from kafka import KafkaConsumer
import base64
import os
from dotenv import load_dotenv
import json
from io import BytesIO
from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg
from dataclasses import dataclass

load_dotenv()
# Kafka configuration

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

CFG_DL_PATH = os.path.join(PROJECT_ROOT, "config", "datalake.yaml")
datalake_cfg = load_cfg(CFG_DL_PATH)["datalake"]

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

YEAR = os.getenv("YEAR_TEST")
MONTH = os.getenv("MONTH_TEST")
DAY = os.getenv("DAY_TEST")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_IMAGE") + f"_{YEAR}_{MONTH}_{DAY}"

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]

BUCKET_NAME = datalake_cfg["bucket_name_1"]
PREFIX = "image"


@dataclass
class Args:
    minio_endpoint: str = MINIO_ENDPOINT
    minio_access_key: str = MINIO_ACCESS_KEY
    minio_secret_key: str = MINIO_SECRET_KEY
    bucket_name: str = BUCKET_NAME
    prefix: str = "image"
    kafka_topic: str = KAFKA_TOPIC
    bootstrap_servers: str = "localhost:9092"


def image_to_dl(args: Args):
    """
    Lấy dữ liệu từ Kafka và lưu vào MinIO
    """
    minio_client = MinIOClient(
        args.minio_endpoint, args.minio_access_key, args.minio_secret_key
    )
    minio_client.create_bucket(args.bucket_name)

    consumer = KafkaConsumer(
        args.kafka_topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print("Start consuming messages...")
    for message in consumer:
        # Decode the message value

        metadata = message.value["metadata"]
        year = metadata["year"]
        month = metadata["month"]
        day = metadata["day"]
        image_data = base64.b64decode(message.value["image_data"])

        # Generate a unique filename
        image_filename = os.path.join(
            f"{args.prefix}/{year}/{month}/{day}/", f"{metadata["id"]}.jpg"
        )
        try:
            # Upload dữ liệu từ nhị phân
            image_data_io = BytesIO(image_data)

            conn = minio_client.create_conn()
            conn.put_object(
                bucket_name=args.bucket_name,
                object_name=image_filename,
                data=image_data_io,
                length=len(image_data),
                content_type="image/jpeg",  # MIME type cho MP3
            )
            print(f"File đã được lưu thành công vào MinIO: {image_filename}")
        except Exception as err:
            print(f"Lỗi khi lưu file lên MinIO: {err}")


if __name__ == "__main__":
    args = Args()
    image_to_dl(args)

# python ./src/stream_processing/streaming_image_to_dl.py

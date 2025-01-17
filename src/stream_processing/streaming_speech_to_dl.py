from kafka import KafkaConsumer
import base64
import os
from dotenv import load_dotenv
import json
from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg
from io import BytesIO

load_dotenv()

from dataclasses import dataclass

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

CFG_DL_PATH = os.path.join(PROJECT_ROOT, "config", "datalake.yaml")
datalake_cfg = load_cfg(CFG_DL_PATH)["datalake"]

YEAR = os.getenv("YEAR_TEST")
MONTH = os.getenv("MONTH_TEST")
DAY = os.getenv("DAY_TEST")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SPEECH") + f"_{YEAR}_{MONTH}_{DAY}"

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]

BUCKET_NAME = datalake_cfg["bucket_name_1"]
PREFIX = "speech"


@dataclass
class Args:
    minio_endpoint: str = MINIO_ENDPOINT
    minio_access_key: str = MINIO_ACCESS_KEY
    minio_secret_key: str = MINIO_SECRET_KEY
    bucket_name: str = BUCKET_NAME
    prefix: str = PREFIX
    kafka_topic: str = KAFKA_TOPIC
    bootstrap_servers: str = "localhost:9092"


def speech_to_dl(args: Args):
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
        speech_data = base64.b64decode(message.value["speech_data"])

        # Generate a unique filename
        speech_filename = os.path.join(
            f"{args.prefix}/{year}/{month}/{day}/" f"{metadata["id"]}.mp3"
        )

        try:
            # Upload dữ liệu từ nhị phân

            speech_data_io = BytesIO(speech_data)

            conn = minio_client.create_conn()
            conn.put_object(
                bucket_name=args.bucket_name,
                object_name=speech_filename,
                data=speech_data_io,
                length=len(speech_data),
                content_type="audio/mpeg",  # MIME type cho MP3
            )
            print(f"File đã được lưu thành công vào MinIO: {speech_filename}")
        except Exception as err:
            print(f"Lỗi khi lưu file lên MinIO: {err}")


if __name__ == "__main__":
    args = Args()
    speech_to_dl(args)

# py ./src/stream_processing/streaming_speech_to_dl.py
#Bu
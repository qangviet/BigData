from kafka import KafkaConsumer
import base64
import os
from dotenv import load_dotenv
import json
from io import BytesIO
from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg

load_dotenv()
# Kafka configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_IMAGE")
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

CFG_DL_PATH = os.path.join(PROJECT_ROOT, "config", "datalake.yaml")
datalake_cfg = load_cfg(CFG_DL_PATH)["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]

BUCKET_NAME = datalake_cfg["bucket_name_1"]
PREFIX = "image"
if __name__ == "__main__":
    minio_client = MinIOClient(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    minio_client.create_bucket(BUCKET_NAME)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    print("Start consuming messages...")
    for message in consumer:
        # Decode the message value

        metadata = message.value["metadata"]
        year = metadata["year"]
        month = metadata["month"]
        image_data = base64.b64decode(message.value["image_data"])

        # Generate a unique filename
        image_filename = os.path.join(
            f"{PREFIX}/{year}/{month}/", f"{metadata["id"]}.jpg"
        )

        try:
            # Upload dữ liệu từ nhị phân
            image_data_io = BytesIO(image_data)

            conn = minio_client.create_conn()
            conn.put_object(
                bucket_name=BUCKET_NAME,
                object_name=image_filename,
                data=image_data_io,
                length=len(image_data),
                content_type="image/jpeg",  # MIME type cho MP3
            )
            print(f"File đã được lưu thành công vào MinIO: {image_filename}")
        except Exception as err:
            print(f"Lỗi khi lưu file lên MinIO: {err}")

# python ./src/stream_processing/streaming_image_to_dl.py

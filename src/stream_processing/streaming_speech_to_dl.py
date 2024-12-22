from kafka import KafkaConsumer
import base64
import os
from dotenv import load_dotenv
import json
from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg
from io import BytesIO

load_dotenv()
# Kafka configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SPEECH")
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
OUTPUT_DIR = "./speech"


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

CFG_DL_PATH = os.path.join(PROJECT_ROOT, "config", "datalake.yaml")
datalake_cfg = load_cfg(CFG_DL_PATH)["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]

BUCKET_NAME = datalake_cfg["bucket_name_1"]
PREFIX = "speech"
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
        speech_data = base64.b64decode(message.value["speech_data"])

        # Generate a unique filename
        speech_filename = os.path.join(
            f"{PREFIX}/{year}/{month}/" f"{metadata["id"]}.mp3"
        )

        try:
            # Upload dữ liệu từ nhị phân

            speech_data_io = BytesIO(speech_data)

            conn = minio_client.create_conn()
            conn.put_object(
                bucket_name=f"{BUCKET_NAME}",
                object_name=speech_filename,
                data=speech_data_io,
                length=len(speech_data),
                content_type="audio/mpeg",  # MIME type cho MP3
            )
            print(f"File đã được lưu thành công vào MinIO: {speech_filename}")
        except Exception as err:
            print(f"Lỗi khi lưu file lên MinIO: {err}")

# py ./src/stream_processing/streaming_speech_to_dl.py

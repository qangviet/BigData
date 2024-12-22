from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
import json
import time
from datetime import datetime
import pandas as pd

BOOTSTRAP_SERVERS = ["localhost:9092"]

# topic_streaming_1 = "streaming.public.yellow_trip_raw"
topic_streaming_2 = "streaming.public.green_trip_raw"
consumer_2 = KafkaConsumer(
    # topic_streaming_1,
    topic_streaming_2,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",  # Hoặc 'latest'
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # Giải mã JSON
)


def extract_data(consumer):
    for message in consumer_2:
        record = message.value
        payload = record.get("payload", {})

        # Tách dữ liệu bản ghi mới
        after_data = payload.get("after", None)
        if after_data:
            print("New record data:", after_data)


if __name__ == "__main__":
    topic_streaming_2 = "streaming.public.green_trip_raw"
    consumer_2 = KafkaConsumer(
        topic_streaming_2,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",  # Hoặc 'latest'
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # Giải mã JSON
    )
    extract_data(consumer_2)

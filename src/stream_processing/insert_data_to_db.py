import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import random
import time
import sys
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import logging
import base64
import zipfile
import json
from kafka.errors import KafkaError
import io
from dataclasses import dataclass

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOOTSTRAP_SERVERS = ["localhost:9092"]

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


DATA_DIR = os.path.join(PROJECT_ROOT, "data")

FILE_TEST = os.path.join(DATA_DIR, os.getenv("FILE_TEST_STREAMING"))
YEAR = os.getenv("YEAR_TEST")
MONTH = os.getenv("MONTH_TEST")
TABLE_GREEN = os.getenv("TABLE_NAME_CDC_GREEN")
TABLE_YELLOW = os.getenv("TABLE_NAME_CDC_YELLOW")
IMAGE_TEST = os.path.join(DATA_DIR, os.getenv("IMAGE_TEST"))
SPEECH_TEST = os.path.join(DATA_DIR, os.getenv("SPEECH_TEST"))

DTYPE_MAPPING = {
    "int32": "INTEGER",  # ánh xạ từ int32 sang INTEGER
    "int64": "BIGINT",  # ánh xạ từ int64 sang BIGINT
    "float64": "DOUBLE PRECISION",  # ánh xạ từ float64 sang DOUBLE PRECISION
    "object": "TEXT",  # ánh xạ từ object sang TEXT
    "datetime64[us]": "TIMESTAMP",
    "datetime64[ns]": "TIMERPSTAMP",  # ánh xạ từ datetime64 sang TIMESTAMP
}

KAFKA_TOPIC_IMAGE = os.getenv("KAFKA_TOPIC_IMAGE")
KAFKA_TOPIC_SPEECH = os.getenv("KAFKA_TOPIC_SPEECH")

MAX_MESSAGE_SIZE = 1024 * 1024 * 5  # 5MB


def countdown_sleep(seconds):
    for remaining in range(seconds, 0, -1):
        sys.stdout.write(f"\rWaiting for {remaining} seconds...")
        sys.stdout.flush()
        time.sleep(1)


def check_table_exists(conn, schema_name, table_name):
    cursor = conn.cursor()
    query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    );
    """
    cursor.execute(query, (schema_name, table_name))
    result = cursor.fetchone()
    cursor.close()
    return result[0]


def clear_all_tables(conn):
    cur = conn.cursor()
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    tables = cur.fetchall()
    for table in tables:
        cur.execute(f"DROP TABLE {table[0]} CASCADE")
    conn.commit()
    cur.close()


def create_table(conn, table_name, columns):
    if check_table_exists(conn, "public", table_name):
        print("Exists table:", table_name)
        return
    cur = conn.cursor()
    columns_str = ", ".join(
        [
            f"{col_name} {DTYPE_MAPPING[str(col_type)]}"
            for col_name, col_type in columns.items()
        ]
    )

    cur.execute(f"CREATE TABLE {table_name} ({columns_str})")
    conn.commit()
    cur.close()
    print("Created table:", table_name)


def insert_to_db(conn, table_name, df):
    cur = conn.cursor()
    for i, row in df.iterrows():
        columns = ", ".join(row.keys())
        values = ", ".join([f"'{str(value)}'" for value in row.values])
        cur.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
    conn.commit()
    cur.close()


def create_topic(admin, topic_name, num_broker=3):
    """
    Create topic if not exists
    """
    try:
        topic = NewTopic(
            name=topic_name, num_partitions=num_broker, replication_factor=num_broker
        )
        admin.create_topic([topic])
        logging.info(f"A new topic {topic_name} has been created!")
    except Exception:
        logging.info(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def simulate_data(
    conn, table_name, range_batch_size=[200, 1000], range_time_sleep=[60, 180]
):
    df = pd.read_parquet(FILE_TEST, engine="pyarrow")
    create_table(conn, table_name, df.dtypes)
    try:
        batch_size = random.randint(range_batch_size[0], range_batch_size[1])
        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i : i + batch_size]
            insert_to_db(conn, table_name, df_batch)
            print(f"Insert {len(df_batch)} rows to {table_name}")
            time_sleep = random.randint(range_time_sleep[0], range_time_sleep[1])
            countdown_sleep(time_sleep)
    except Exception as e:
        print("Error: ", e)


import json


def send_image_file(file_path, producer, id, topic="raw_image"):
    """Reads an image file, base64 encodes it, and sends it to Kafka."""
    try:
        if not file_path or not isinstance(file_path, str):
            logging.error("Invalid file_path provided")
            return
        if not id:
            logging.error("Invalid id provided")
            return

        with open(file_path, "rb") as f:
            image_data = f.read()

        payload = base64.b64encode(image_data).decode("utf-8")
        message_data = {
            "image_data": payload,
            "metadata": {"id": id, "year": YEAR, "month": MONTH},
        }
        message_bytes = json.dumps(message_data).encode("utf-8")

        message_size = len(message_bytes)
        if message_size > MAX_MESSAGE_SIZE:
            print(
                f"Message size {message_size / (1024 * 1024):.2f} MB exceeds 5 MB limit"
            )
            return
        producer.send(topic, value=message_bytes)
        # print("Send image to Kafka")
        logging.info(f"\nSent image file {file_path} with ID: {id} to topic: {topic}")
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except KafkaError as e:
        logging.error(
            f"Kafka error while sending file {file_path} with ID: {id}. Error: {e}"
        )
    except Exception as e:
        logging.error(
            f"Unexpected error sending file {file_path} with ID: {id}. Error: {e}"
        )


def send_mp3_file(file_path, producer, id, topic="raw_speech"):
    """Reads a .mp3 file, base64 encodes it, and sends it to Kafka."""
    try:
        if not file_path or not isinstance(file_path, str):
            logging.error("Invalid file_path provided")
            return
        if not id:
            logging.error("Invalid id provided")
            return

        with open(file_path, "rb") as f:
            mp3_data = f.read()

        payload = base64.b64encode(mp3_data).decode("utf-8")
        message_data = {
            "speech_data": payload,
            "metadata": {"id": id, "year": YEAR, "month": MONTH},
        }
        message_bytes = json.dumps(message_data).encode("utf-8")
        if len(message_bytes) > MAX_MESSAGE_SIZE:
            logging.error(
                f"Message size {len(message_bytes) / (1024 * 1024):.2f} MB exceeds 5 MB limit"
            )
            return
        producer.send(topic, value=message_bytes)
        # print("Send mp3 to Kafka")
        logging.info(f"\nSent mp3 file {file_path} with ID: {id} to topic: {topic}")
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except KafkaError as e:
        logging.error(
            f"Kafka error while sending file {file_path} with ID: {id}. Error: {e}"
        )
    except Exception as e:
        logging.error(
            f"Unexpected error sending file {file_path} with ID: {id}. Error: {e}"
        )


@dataclass
class Args:
    table_name: str = TABLE_GREEN
    time_sleep: int = 5
    file_path: str = FILE_TEST
    bootstrap_servers: str = ".".join(BOOTSTRAP_SERVERS)
    topic_image: str = KAFKA_TOPIC_IMAGE
    topic_speech: str = KAFKA_TOPIC_SPEECH
    max_message_size: int = MAX_MESSAGE_SIZE
    signal_file: str = None


def simulate_data_with_image(conn, args: Args):
    df = pd.read_parquet(args.file_path, engine="pyarrow")
    create_table(conn, args.table_name, df.dtypes)
    producer = None
    admin = None
    for _ in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                max_request_size=args.max_message_size,  # 5MB
            )
            admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_servers)
            print("Connected to Kafka")
            break
        except Exception as e:
            logging.info(
                f"Trying to instantiate admin and producer with bosootstrap server {args.bootstrap_servers} with error {e}"
            )
            countdown_sleep(10)
            pass
    create_topic(admin, args.topic_image)
    create_topic(admin, args.topic_speech)

    # try:
    for i in range(0, len(df)):
        row = df.iloc[i : i + 1]
        _id = row["id"]

        insert_to_db(conn, args.table_name, row)
        send_image_file(IMAGE_TEST, producer, _id.iloc[0], topic=KAFKA_TOPIC_IMAGE)
        send_mp3_file(SPEECH_TEST, producer, _id.iloc[0], topic=KAFKA_TOPIC_SPEECH)
        print(f"Insert 1 rows to {args.table_name}\n")
        producer.flush()
        countdown_sleep(args.time_sleep)
    # except Exception as e:
    #     print("Error: ", e)
    conn.close()


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname=os.getenv("DB_STREAM_NAME"),
        user=os.getenv("DB_STREAM_USER"),
        password=os.getenv("DB_STREAM_PASSWORD"),
        host=os.getenv("DB_STREAM_HOST"),
        port=os.getenv("DB_STREAM_PORT"),
    )

    clear_all_tables(conn)
    # simulate_data(conn, TABLE_GREEN)

    args = Args(
        table_name=TABLE_GREEN,
        time_sleep=5,
        file_path=FILE_TEST,
        bootstrap_servers=".".join(BOOTSTRAP_SERVERS),
        topic_image=KAFKA_TOPIC_IMAGE,
        topic_speech=KAFKA_TOPIC_SPEECH,
        max_message_size=MAX_MESSAGE_SIZE,
    )
    simulate_data_with_image(conn, args)


# py ./src/stream_processing/insert_data_to_db.py
#VuDinhToan da den day

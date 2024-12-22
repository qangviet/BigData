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

load_dotenv()

BOOTSTRAP_SERVERS = ["localhost:9092"]

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


DATA_DIR = os.path.join(PROJECT_ROOT, "data")

FILE_TEST = os.path.join(DATA_DIR, os.getenv("FILE_TEST_STREAMING"))
print(FILE_TEST)
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


def simulate_data_with_image(conn, table_name, time_sleep=2):
    df = pd.read_parquet(FILE_TEST, engine="pyarrow")
    create_table(conn, table_name, df.dtypes)
    producer = None
    admin = None
    for _ in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            print("Connected to Kafka")
            break
        except Exception as e:
            logging.info(
                f"Trying to instantiate admin and producer with bosootstrap server {servers} with error {e}"
            )
            countdown_sleep(10)
            pass
    create_topic(admin, "raw_image_speech")

    with open(IMAGE_TEST, "rb") as image_file:
        image_data = image_file.read()
        image_base64 = base64.b64encode(image_data).decode("utf-8")
    with open(SPEECH_TEST, "rb") as audio_file:
        audio_data = audio_file.read()
        audio_base64 = base64.b64encode(audio_data).decode("utf-8")

    # try:
    for i in range(0, len(df)):
        row = df.iloc[i : i + 1]
        _id = row["id"]

        insert_to_db(conn, table_name, row)
        print(f"\nInsert 1 rows to {table_name}")
        message_data = {
            "image_data": image_base64,
            # "audio_data": audio_base64,
            "metadata": {"id": _id.iloc[0]},
        }
        message_bytes = json.dumps(message_data).encode("utf-8")
        producer.send("raw_image_speech", value=message_bytes)
        # producer.flush()
        print("Sent message(image, mp3) to Kafka")
        countdown_sleep(time_sleep)
    # except Exception as e:
    #     print("Error: ", e)


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
    simulate_data_with_image(conn, TABLE_GREEN, time_sleep=2)
    conn.close()

# py ./src/stream_processing/insert_data_to_db.py

import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
import random
import time
import sys

load_dotenv()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")

FILE_TEST = os.path.join(DATA_DIR, os.getenv("FILE_TEST_STREAMING"))

TABLE_GREEN = os.getenv("TABLE_NAME_CDC_GREEN")
TABLE_YELLOW = os.getenv("TABLE_NAME_CDC_YELLOW")

DTYPE_MAPPING = {
    "int32": "INTEGER",  # ánh xạ từ int32 sang INTEGER
    "int64": "BIGINT",  # ánh xạ từ int64 sang BIGINT
    "float64": "DOUBLE PRECISION",  # ánh xạ từ float64 sang DOUBLE PRECISION
    "object": "TEXT",  # ánh xạ từ object sang TEXT
    "datetime64[us]": "TIMESTAMP",
    "datetime64[ns]": "TIMESTAMP",  # ánh xạ từ datetime64 sang TIMESTAMP
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


def simulate_data_to_db(
    conn, table_name, range_batch_size=[200, 1000], range_time_sleep=[60, 180]
):
    df = df = pd.read_parquet(FILE_TEST, engine="pyarrow")
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


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname=os.getenv("DB_STREAM_NAME"),
        user=os.getenv("DB_STREAM_USER"),
        password=os.getenv("DB_STREAM_PASSWORD"),
        host=os.getenv("DB_STREAM_HOST"),
        port=os.getenv("DB_STREAM_PORT"),
    )
    simulate_data_to_db(conn, TABLE_GREEN)
    conn.close()

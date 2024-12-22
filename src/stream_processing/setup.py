import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DTYPE_MAPPING = {
    "int32": "INTEGER",  # ánh xạ từ int32 sang INTEGER
    "int64": "BIGINT",  # ánh xạ từ int64 sang BIGINT
    "float64": "DOUBLE PRECISION",  # ánh xạ từ float64 sang DOUBLE PRECISION
    "object": "TEXT",  # ánh xạ từ object sang TEXT
    "datetime64[us]": "TIMESTAMP",
    "datetime64[ns]": "TIMESTAMP",  # ánh xạ từ datetime64 sang TIMESTAMP
}


def clear_all_tables(conn):
    cur = conn.cursor()
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    tables = cur.fetchall()
    for table in tables:
        cur.execute(f"DROP TABLE {table[0]} CASCADE")
    conn.commit()
    cur.close()


def create_table(conn, table_name, columns):
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


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname=os.getenv("DB_STREAM_NAME"),
        user=os.getenv("DB_STREAM_USER"),
        password=os.getenv("DB_STREAM_PASSWORD"),
        host=os.getenv("DB_STREAM_HOST"),
        port=os.getenv("DB_STREAM_PORT"),
    )
    clear_all_tables(conn)

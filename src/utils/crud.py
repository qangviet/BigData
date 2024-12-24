#MAPPING
DTYPE_MAPPING = {
    "int32": "INTEGER",  # ánh xạ từ int32 sang integer
    "int64": "BIGINT",  # ánh xạ từ int64 sang bigint
    "float64": "DOUBLE PRECISION",  # ánh xạ từ float64 sang double precision
    "object": "TEXT",  # ánh xạ từ object sang text
    "datetime64[us]": "TIMESTAMP",
    "datetime64[ns]": "TIMESTAMP",  # ánh xạ từ datetime64 sang timestamp
}


def create_raw_table(conn, table_name, columns):
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


def insert_to_db(conn, table_name, df):
    cur = conn.cursor()
    for i, row in df.iterrows():
        columns = ", ".join(row.keys())
        values = ", ".join([f"'{str(value)}'" for value in row.values])
        cur.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
    conn.commit()
    cur.close()

import streamlit as st
import pandas as pd
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Cấu hình
DATA_DIR = "./data"  # Đường dẫn đến thư mục chứa file dữ liệu
FILE_EXT = ".csv"  # Phần mở rộng của file dữ liệu
REFRESH_INTERVAL = 30  # Kiểm tra mỗi 30 giây

# Biến toàn cục để lưu DataFrame hiện tại
current_df = None
is_initial_load = True


def load_data(file_path):
    """Loads data from a CSV file."""
    global current_df
    try:
        if file_path.lower().endswith(".csv"):
            current_df = pd.read_csv(file_path)
        elif file_path.lower().endswith(".parquet"):
            current_df = pd.read_parquet(file_path)
        else:
            st.error(f"Unsupported file type for {file_path}!")
    except Exception as e:
        st.error(f"Error loading file: {e}")


# Hàm để tìm file dữ liệu mới nhất
def find_latest_data_file():
    if not os.path.exists(DATA_DIR):
        return None, None

    files = [f for f in os.listdir(DATA_DIR) if f.endswith(FILE_EXT)]
    if not files:
        return None, None

    files_with_paths = [os.path.join(DATA_DIR, f) for f in files]

    if not files_with_paths:
        return None, None

    latest_file = max(files_with_paths, key=os.path.getmtime)

    return latest_file, os.path.getmtime(latest_file)


# Handler để xử lý sự kiện thay đổi file
class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, refresh_callback):
        self.refresh_callback = refresh_callback
        super().__init__()

    def on_created(self, event):
        if event.is_directory:
            return None
        self.refresh_callback()

    def on_modified(self, event):
        if event.is_directory:
            return None
        self.refresh_callback()


def streamlit_app():
    global current_df, is_initial_load
    st.title("Real-time Data Visualization")

    while True:
        latest_file, file_time = find_latest_data_file()
        if latest_file and (
            current_df is None
            or (
                file_time is not None
                and current_df is not None
                and file_time > os.path.getmtime(latest_file)
                and not is_initial_load
            )
        ):
            with st.spinner(f"Loading {os.path.basename(latest_file)}..."):
                load_data(latest_file)
                is_initial_load = False  # Disable initial load after file is loaded
        if current_df is not None:
            st.write("### Data:")
            st.dataframe(current_df)

        time.sleep(REFRESH_INTERVAL)


def main():
    streamlit_app()


if __name__ == "__main__":
    main()

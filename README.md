# Bài tập lớn Lưu trữ và xử lý dữ liệu 154050 
## _Taxi data pipeline for Transportation Company_

Nhóm 9:
- Nguyễn Quang Huy 20215396
- Trương Quang Việt 20215515
- Đinh Nhật Ký 20215410
- Lường Mạnh Tú 20215500
- Vũ Đình Toản - 20215480


## Run
### Airflow:
```sh
cd to-project
docker-compose -p airflow_project -f airflow-docker-compose.yml up -d --build
```

### Docker Streaming Data: 
```sh
cd to-project
3 broker: docker-compose -p streaming_project -f stream-docker-compose-3.yml up -d --build
1 broker: docker-compose -p streaming_project -f stream-docker-compose-1.yml up -d --build
```
### Docker Batch Data:
```sh
cd to-project
docker-compose -p batch_project -f docker-compose.yml up -d --build
```

### Debezium config:
```sh
cd debezium
bash run.sh register_connector configs/taxi-nyc-cdc.json
```

### Upload file:
```sh
py ./src/batch_processing/upload_from_local.py
```

### Set package - Rất quan trọng trước khi chạy:
```sh
set PYTHONPATH=path/to/file/src;%PYTHONPATH% 
```
# Bài tập lớn Lưu trữ và xử lý dữ liệu 154050 
## _Taxi data pipeline for Transportation Company_

Nhóm 9:
- Nguyễn Quang Huy 20215396
- Trương Quang Việt 20215515
- Đinh Nhật Ký 20215410
- Lường Mạnh Tú 20215500
- Vũ Đình Toản - 20215480


## Link báo cáo bài tập lớn: [Link](https://docs.google.com/document/d/1wi25bE1VdXqGXZXwTzkoc0o8o1jEGUUjqB4PH4KO8Fs/edit?fbclid=IwZXh0bgNhZW0CMTAAAR2EpgJir8243x2x1ditwo11MMnvznjKb4m-daGhIUvN_X692NAibPP6Qss_aem_bEfaekpK_CHqDdTyz1ze8g&tab=t.0)



## Link slide: [Link](https://docs.google.com/presentation/d/1kQfi-zL7AWUoAzZumrRavFNEbdyZmq2uRGFWOQkF0t0/edit?fbclid=IwZXh0bgNhZW0CMTAAAR0dJZz_xJIgMWbSseyfbSZH9ZBwL4ovsc-moEUyyJqFkr34BntleKs1rAM_aem_UHLtuVadod0wP7G6nZvyJQ#slide=id.g3177a44d7b8_2_57)

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

### Set package:
```sh
set_pythonpath.bat
```

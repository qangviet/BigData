@echo off
rem create jars for spark
mkdir jars
cd jars
curl -O https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.30.0/spark-bigquery-with-dependencies_2.12-0.30.0.jar
curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.4.3/postgresql-42.4.3.jar
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar
curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.3/spark-streaming-kafka-0-10_2.12-3.5.3.jar
echo File downloaded successfully!

rem set PYTHONPATH
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%src;%PYTHONPATH%"
echo PYTHONPATH=%PYTHONPATH%

pause

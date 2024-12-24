FROM apache/airflow:2.9.2

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/

# Set current project root
ENV PROJECT_BIGDATA_ROOT E:/BigData_2/MyProject


RUN export JAVA_HOME

RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y libgomp1 

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
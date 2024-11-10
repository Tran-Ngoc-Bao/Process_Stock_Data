FROM apache/airflow
USER root
RUN apt update
RUN apt install -y default-jdk 
USER airflow
RUN pip install pyspark
RUN pip install minio
ARG AIRFLOW_IMAGE_VERSION
FROM apache/airflow:${AIRFLOW_IMAGE_VERSION}

USER root
RUN apt update
RUN apt install -y default-jdk

USER airflow
ADD requirements.txt .
RUN pip install -r requirements.txt

USER root
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh
USER airflow

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
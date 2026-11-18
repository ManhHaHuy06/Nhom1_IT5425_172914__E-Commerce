FROM apache/airflow:2.10.5-python3.11

USER root
RUN mkdir -p /opt/airflow/work && chown -R airflow:root /opt/airflow/work

USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    pymysql==1.1.1 pyarrow boto3 requests
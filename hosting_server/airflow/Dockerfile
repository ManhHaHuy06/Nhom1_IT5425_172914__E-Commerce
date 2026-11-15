FROM apache/airflow:2.10.5-python3.11
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    pymysql cryptography pyarrow boto3 sqlalchemy pandas

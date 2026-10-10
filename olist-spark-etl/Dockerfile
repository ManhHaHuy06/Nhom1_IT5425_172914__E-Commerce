FROM apache/spark:3.5.6-python3

USER root

ENV PATH="/opt/spark/bin:${PATH}"

COPY jobs /opt/olist/jobs
COPY config /opt/olist/config
COPY docs /opt/olist/docs

WORKDIR /opt/olist

CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "/opt/olist/jobs/etl.py"]
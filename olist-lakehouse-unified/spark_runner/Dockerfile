ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.5.0
FROM apache/spark:${SPARK_VERSION}
ARG ICEBERG_VERSION
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl wget python3-pip \
 && pip3 install --no-cache-dir fastapi==0.115.6 uvicorn==0.33.0 pydantic==2.10.4 \
 && rm -rf /var/lib/apt/lists/*
RUN wget -q -P /opt/spark/jars/ "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
 && wget -q -P /opt/spark/jars/ "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar" \
 && wget -q -P /opt/spark/jars/ "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
 && wget -q -P /opt/spark/jars/ "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
 && wget -q -P /opt/spark/jars/ "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar"
ENV SPARK_HOME=/opt/spark PYTHONPATH=/opt/olist AWS_REGION=us-east-1
WORKDIR /opt/olist
COPY runner_api.py /opt/runner/runner_api.py
EXPOSE 8090
USER 185
CMD ["python3", "-m", "uvicorn", "--app-dir", "/opt/runner", "runner_api:app", "--host", "0.0.0.0", "--port", "8090"]

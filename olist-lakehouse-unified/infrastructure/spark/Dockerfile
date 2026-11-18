# Khai báo phiên bản và Image
ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.5.0

FROM apache/spark:${SPARK_VERSION}

ARG ICEBERG_VERSION

USER root

# Chuẩn bị môi trường cài đặt
RUN apt-get update && apt-get install -y curl wget && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV SPARK_JARS_DIR=${SPARK_HOME}/jars

# Tải 5 thư viện JAR

# Cung cấp engine tích hợp Iceberg cho Spark 3.5
RUN wget -q -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# Bộ SDK thế hệ thứ 2 của AWS cho Java.
RUN wget -q -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar"

# Cung cấp lớp kết nối FileSystem chuẩn của Hadoop
RUN wget -q -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

# Bộ SDK thế hệ thứ nhất của AWS.
RUN wget -q -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Module tích hợp riêng giữa Apache Iceberg và hệ sinh thái AWS/S3
RUN wget -q -P ${SPARK_JARS_DIR}/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar"

COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

USER spark

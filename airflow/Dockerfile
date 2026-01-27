FROM apache/airflow:3.1.6

# Java configuration
ARG JAVA_VERSION=21
USER root
RUN apt-get update && apt install openjdk-${JAVA_VERSION}-jdk-headless -y --no-install-recommends
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# PySpark configuration
ARG PYSPARK_VERSION=4.0.1
USER airflow
RUN pip install pyspark==${PYSPARK_VERSION}
ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark

FROM apache/airflow:3.2.2-python3.12

COPY requirements.txt /opt/airflow/

USER root

# Install system dependencies + Java (headless)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        openjdk-17-jre-headless \
        ca-certificates-java && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN JAVA_BIN=$(readlink -f $(which java)) && \
    JAVA_HOME=$(dirname $(dirname $JAVA_BIN)) && \
    echo "JAVA_HOME=${JAVA_HOME}" >> /etc/environment && \
    echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile.d/java.sh  # FIX 5.2

ARG DETECTED_JAVA_HOME
RUN JAVA_BIN=$(readlink -f $(which java)) && \
    echo "JAVA_HOME=$(dirname $(dirname $JAVA_BIN))" > /tmp/java_home.env  # FIX 5.2
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

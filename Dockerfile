# Base image Airflow 2.10.5 + Python 3.12
FROM apache/airflow:2.10.5-python3.12

# Cài Java Temurin JDK 11
USER root
RUN apt-get update && apt-get install -y wget gnupg2 \
  && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor > /usr/share/keyrings/adoptium.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list \
  && apt-get update && apt-get install -y temurin-11-jdk \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường Java
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV CLASSPATH=/opt/airflow/jars:.

# Tạo thư mục chứa file JAR
RUN mkdir -p /opt/airflow/jars

# Copy file .jar vào container
COPY jars/*.jar /opt/airflow/jars/

# Trở lại user airflow
USER airflow

# Copy requirements.txt và cài đặt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


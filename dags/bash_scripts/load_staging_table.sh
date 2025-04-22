spark-submit --driver-class-path $SPARK_HOME/jars/postgresql-42.7.4.jar \
--jars $SPARK_HOME/jars/postgresql-42.7.4.jar \
$AIRFLOW_HOME/dags/python_scripts/{{params.python_script}} {{params.minio_bucket}} {{params.minio_folder}} \
{{params.minio_key}} {{params.minio_secret_key}} {{params.postgres_conn_string}} \
{{params.db_user}} {{params.db_pass}} --conf "fs.s3a.multipart.size=104857600" 

#!/bin/bash

echo "=== DEBUG PARAMS ==="
echo "MINIO_FOLDER: {{ params.minio_folder }}"
echo "DB_USER: {{ params.db_user }}"
echo "DB_PASS: {{ params.db_pass }}"
echo "POSTGRES_CONN_STRING: {{ params.postgres_conn_string }}"
echo "MINIO_BUCKET: {{ params.minio_bucket }}"
echo "MINIO_SECRET_KEY: {{ params.minio_secret_key }}"
echo "MINIO_KEY: {{ params.minio_key }}"
echo "PYTHON_SCRIPT: {{ params.python_script }}"
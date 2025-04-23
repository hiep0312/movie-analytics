spark-submit --driver-class-path $SPARK_HOME/jars/postgresql-42.7.4.jar \
--jars $SPARK_HOME/jars/postgresql-42.7.4.jar \
$AIRFLOW_HOME/dags/python_scripts/{{params.python_script}} {{params.s3_bucket}} {{params.s3_key}} \
{{params.aws_key}} {{params.aws_secret_key}} {{params.postgres_conn_string}} \
{{params.db_user}} {{params.db_pass}} --conf "fs.s3a.multipart.size=104857600" 

#!/bin/bash

echo "=== DEBUG PARAMS ==="
echo "s3_key: {{ params.s3_key }}"
echo "DB_USER: {{ params.db_user }}"
echo "DB_PASS: {{ params.db_pass }}"
echo "POSTGRES_CONN_STRING: {{ params.postgres_conn_string }}"
echo "s3_bucket: {{ params.s3_bucket }}"
echo "aws_secret_key: {{ params.aws_secret_key }}"
echo "aws_key: {{ params.aws_key }}"
echo "PYTHON_SCRIPT: {{ params.python_script }}"
spark-submit --driver-class-path /postgresql-42.7.4.jar \
--jar /postgresql-42.7.4.jar \
$AIRFLOW_HOME/dags/python_scripts/{{params.python_script}} {{params.minio_bucket}} {{params.minio_user}} \
{{params.minio_key}} {{params.minio_secret_key}} {{params.postgres_conn_string}} \
{{params.db_user}} {{params.db_pass}} --conf "fs.s3a.multipart.size=104857600"
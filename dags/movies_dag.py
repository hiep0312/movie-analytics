import os
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

movie_config = Variable.get("movie_config", deserialize_json=True)
postgres_conn_id = movie_config["db_connect_id"]

params = {'minio_key': movie_config["minio_access"],
          'minio_secret_key': movie_config["minio_secret_key"],
          'db_user': Variable.get("db_user"),
          'db_pass': Variable.get("db_pass"),
          'postgres_conn_string': movie_config["db_connect_id"],
          'minio_bucket': movie_config["minio_bucket"],
          'minio_user': movie_config["minio_user"]  
         }

# Default settings for DAG
default_args = {
    'owner': 'Alan',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id='movie_dag', default_args=default_args,
         description='Load and transform data', schedule_interval='@once') as dag:
    
    start_operator = EmptyOperator(task_id='begin-execution', dag=dag)

    # create tables
    create_tables = SQLExecuteQueryOperator(task_id='create_tables', conn_id=postgres_conn_id, sql="sql_scripts/create_tables.sql", dag=dag)

    # Load stage_ratings data table
    params['python_script'] = 'load_staging_ratings.py'
    load_staging_ratings = BashOperator(task_id='load-staging-ratings',
                                        bash_command= './bash_scripts/load_staging_table.sh',
                                        params=params,
                                        dag=dag)

    start_operator >> create_tables

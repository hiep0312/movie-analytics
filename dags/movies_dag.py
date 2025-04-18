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


postgres_conn_id = 'post'


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

    start_operator >> create_tables

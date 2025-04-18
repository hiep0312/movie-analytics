from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

def hello_world():
    return "hello world"

with DAG(dag_id='test_dag', description='test function for dag', start_date=datetime(2025, 4, 17), schedule_interval='@once') as dag:
    start_process = EmptyOperator(task_id='start_process')

    hello = PythonOperator(task_id='hello_world', python_callable=hello_world)

    start_process >> hello
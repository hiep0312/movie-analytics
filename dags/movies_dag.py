from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from data_quality import DataQualityOperator
from airflow.models import Variable

movie_config = Variable.get("movie_config", deserialize_json=True)
postgres_conn_id = movie_config["db_connect_id"]

params = {'minio_folder': movie_config["minio_folder"],
          'db_user': Variable.get("db_user"),
          'db_pass': Variable.get("db_pass"),
          'postgres_conn_string': movie_config["postgres_conn_string"],
          'minio_bucket': movie_config["minio_bucket"],
          'minio_secret_key': movie_config["minio_secret_key"],
          'minio_key': movie_config["minio_key"]  
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
    
    # # Load stage_movies data table
    # params['python_script'] = 'load_staging_movies.py'
    # load_staging_movies = BashOperator(task_id='load-staging-movies',
    #                                    bash_command= './bash_scripts/load_staging_table.sh',
    #                                    params=params,
    #                                    dag=dag)

    # # Load stage_cpi data table
    # params['python_script'] = 'load_staging_cpi.py'
    # load_staging_cpi = BashOperator(task_id='load-staging-cpi',
    #                                 bash_command= './bash_scripts/load_staging_table.sh',
    #                                 params=params,
    #                                 dag=dag)
    
    # # Load stage_genre data table
    # params['python_script'] = 'load_staging_genre.py'
    # load_staging_genre = BashOperator(task_id='load-staging-genre',
    #                                   bash_command= './bash_scripts/load_staging_table.sh',
    #                                   params=params,
    #                                   dag=dag)
    
    # # Load stage_date data table
    # params['python_script'] = 'load_staging_date.py'
    # load_staging_date = BashOperator(task_id='load-staging-date',
    #                                  bash_command= './bash_scripts/load_staging_table.sh',
    #                                  params=params,
    #                                  dag=dag)
    
    # upsert_ratings = SQLExecuteQueryOperator(task_id='upsert-ratings-table', conn_id=postgres_conn_id,
    #                                 sql="sql_scripts/upsert_ratings.sql", dag=dag)

    # upsert_movies = SQLExecuteQueryOperator(task_id='upsert-movies-table', conn_id=postgres_conn_id,
    #                                  sql="sql_scripts/upsert_movies.sql", dag=dag)

    # upsert_cpi = SQLExecuteQueryOperator(task_id='upsert-staging-cpi', conn_id=postgres_conn_id,
    #                               sql='sql_scripts/upsert_cpi.sql', dag=dag)

    # upsert_date = SQLExecuteQueryOperator(task_id='upsert-staging-date', conn_id=postgres_conn_id,
    #                               sql='sql_scripts/upsert_date.sql', dag=dag)

    # upsert_genre = SQLExecuteQueryOperator(task_id='upsert-staging-genre', conn_id=postgres_conn_id,
    #                               sql='sql_scripts/upsert_genre.sql', dag=dag)
    
    # # Check for quality issues in ingested data
    # tables = ["movies.movies", "movies.ratings", "movies.movie_genre",
    #           "movies.genre", "movies.date", "movies.cpi"]
    # check_data_quality = DataQualityOperator(task_id='run_data_quality_checks',
    #                                         postgres_conn_id=postgres_conn_id,
    #                                         table_names=tables,
    #                                         dag=dag)


    start_operator >> create_tables
    create_tables  >> load_staging_ratings
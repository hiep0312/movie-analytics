import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pyspark.sql import SparkSession

from dotenv import load_dotenv
from python_scripts.create_tables import create_tables
from python_scripts.load_staging_ratings import load_staging_rating
from python_scripts.load_staging_movies import load_staging_movies
from python_scripts.load_staging_genre import load_staging_genre
from python_scripts.load_staging_date import load_staging_date
from python_scripts.load_staging_cpi import load_staging_cpi

from python_scripts.upsert_cpi import upsert_cpi
from python_scripts.upsert_date import upsert_date
from python_scripts.upsert_genre import upsert_genre
from python_scripts.upsert_movies import upsert_movies
from python_scripts.upsert_ratings import upsert_ratings

from python_scripts.data_quality import check_data_quality
from sqlalchemy import create_engine, text


dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(dag_folder, '..'))
sys.path.insert(0, project_root)


def print_hello(**kwargs):
    print("Hello, Airflow!")


load_dotenv(os.path.join(project_root, '.env'))

postgres_conn_id = os.getenv("POSTGRES_ID")

params = {
    's3_bucket': os.getenv("S3_BUCKET"),
    's3_key': os.getenv("S3_KEY"),
    'db_user': os.getenv("DB_USER"),
    'db_pass': os.getenv("DB_PASSWORD"),
    'postgres_conn_string': os.getenv("POSTGRES_CONN_STRING"),
    'aws_secret_key': os.getenv("AWS_SECRET_KEY"),
    'aws_key': os.getenv("AWS_KEY"),
    'postgres_url': os.getenv("POSTGRES_URL")
}

# Default settings for DAG
default_args = {
    'owner': 'roy',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

def create_spark_session(aws_key, aws_secret_key):
    return SparkSession \
        .builder \
        .config("spark.executor.heartbeatInterval", "40s") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4") \
        .config("fs.s3a.access.key", aws_key) \
        .config("fs.s3a.secret.key", aws_secret_key) \
        .config("fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
        # .config("spark.jars", "jars/postgresql-42.7.4.jar") \

spark = create_spark_session(params['aws_key'], params['aws_secret_key'])

def delete_tables():
    engine = create_engine(params["postgres_conn_string"])

    drop_sql = f"""
DO
$$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN (
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'movies'
    ) LOOP
        EXECUTE format('DROP TABLE IF EXISTS movies.%I CASCADE;', rec.tablename);
    END LOOP;
END;
$$;
    """

    try:
        with engine.connect() as connection:
            connection.execute(text(drop_sql))
    except Exception as e:
        raise



with DAG(dag_id='movilytics', default_args=default_args, description='Load and transform data', schedule_interval='@once', catchup=False) as dag:
    start = EmptyOperator(task_id='start')


    # create tables
    create_tables_task = PythonOperator(task_id='create-tables', 
                                        python_callable=create_tables,
                                        op_kwargs={'params': params},
                                        dag=dag)
   
    load_staging_rating_task = PythonOperator(task_id='load_staging_rating',
                                              python_callable=load_staging_rating,
                                              op_kwargs={'spark': spark,
                                                         'params': params})
    

    load_staging_movies_task = PythonOperator(task_id='load_staging_movies',
                                              python_callable=load_staging_movies,
                                              op_kwargs={'spark': spark,
                                                         'params': params})
    

    load_staging_cpi_task = PythonOperator(task_id='load_staging_cpi',
                                              python_callable=load_staging_cpi,
                                              op_kwargs={'spark': spark,
                                                         'params': params})
    

   
    load_staging_date_task = PythonOperator(task_id='load_staging_date',
                                              python_callable=load_staging_date,
                                              op_kwargs={'spark': spark,
                                                         'params': params})
    

   
    load_staging_genre_task = PythonOperator(task_id='load_staging_genre',
                                              python_callable=load_staging_genre,
                                              op_kwargs={'spark': spark,
                                                         'params': params})
    
    upsert_cpi_task = PythonOperator(task_id='upsert_cpi', 
                                        python_callable=upsert_cpi,
                                        op_kwargs={'params': params},
                                        dag=dag)
    
    upsert_ratings_task = PythonOperator(task_id='upsert_ratings', 
                                        python_callable=upsert_ratings,
                                        op_kwargs={'params': params},
                                        dag=dag)
    
    upsert_movies_task = PythonOperator(task_id='upsert_movies', 
                                        python_callable=upsert_movies,
                                        op_kwargs={'params': params},
                                        dag=dag)
    
    upsert_date_task = PythonOperator(task_id='upsert_date', 
                                        python_callable=upsert_date,
                                        op_kwargs={'params': params},
                                        dag=dag)
    
    upsert_genre_task = PythonOperator(task_id='upsert_genre', 
                                        python_callable=upsert_genre,
                                        op_kwargs={'params': params},
                                        dag=dag)
    
    table_names = ["movies.movies", "movies.ratings", "movies.movie_genre",
              "movies.genre", "movies.date", "movies.cpi"]
    check_data_quality_task = PythonOperator(task_id='check_data_quality',
                                             python_callable=check_data_quality,
                                             op_kwargs={'params': params,
                                                        'table_names': table_names},
                                             dag=dag)

    start >> create_tables_task
    create_tables_task >> [load_staging_rating_task, load_staging_movies_task, load_staging_cpi_task, load_staging_date_task, load_staging_genre_task]
    load_staging_cpi_task >> upsert_cpi_task
    load_staging_date_task >> upsert_date_task
    load_staging_genre_task >> upsert_genre_task
    load_staging_movies_task >> upsert_movies_task
    load_staging_rating_task >> upsert_ratings_task
    [upsert_cpi_task, upsert_date_task, upsert_genre_task, upsert_movies_task, upsert_ratings_task] >> check_data_quality_task
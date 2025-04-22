import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, DateType as Date,
                               BooleanType as Boolean, FloatType as Float,
                               LongType as Long, StringType as String,
                               ArrayType as Array)
from pyspark.sql.functions import col
from datetime import datetime
from pyspark.sql.functions import (col, year, month, dayofmonth, weekofyear, quarter)

def create_spark_session(minio_key, minio_secret_key):
    spark = SparkSession.builder \
        .appName("Spark with MinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.executor.heartbeatInterval", "40s") \
        .config("spark.hadoop.fs.s3a.access.key", minio_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    return spark
    

def format_datetime(ts):
    return datetime.fromtimestamp(ts/1000.0) 

if __name__ == "__main__":
    minio_bucket = sys.argv[1]
    minio_folder = sys.argv[2]
    minio_key = sys.argv[3]
    minio_secret_key = sys.argv[4]
    postgres_conn_string = sys.argv[5]
    db_user = sys.argv[6]
    db_pass = sys.argv[7]

    spark = create_spark_session(minio_key=minio_key,minio_secret_key=minio_secret_key)

    movies_schema = StructType([
        Fld("adult", String()),
        Fld("belongs_to_collection", Long()),
        Fld("budget", Long()),
        Fld("genres", String()),
        Fld("homepage", String()),
        Fld("id", Int()),
        Fld("imdb_id", String()),
        Fld("original_language", String()),
        Fld("original_title", String()),
        Fld("overview", String()),
        Fld("popularity", Dbl()),
        Fld("poster_path", String()),
        Fld("production_company", String()),
        Fld("production_country",  String()),
        Fld("release_date", Date()),
        Fld("revenue", Long()),
        Fld("runtime", Float()),
        Fld("spoken_languages", String()),
        Fld("status", String()),
        Fld("tagline", String()),
        Fld("title", String()),
        Fld("video", Boolean()),
        Fld("vote_average", Float()),
        Fld("vote_count", Int())
    ])

    movies_df = spark.read.option("header", "true") \
                           .csv("s3a://{}/{}/movies_metadata.csv".format(minio_bucket, minio_folder), 
                                schema=movies_schema)
    
    movies_df = movies_df.na.drop()

    date_table = movies_df.select(
                    col('release_date'),
                    dayofmonth("release_date").alias('day'),
                    weekofyear("release_date").alias('week'),
                    month("release_date").alias('month'),
                    quarter("release_date").alias('quarter'),
                    year("release_date").alias('year')
                 ).dropDuplicates()
    
    date_table.write \
              .format("jdbc")  \
              .option("url", postgres_conn_string) \
              .option("dbtable", "movies.stage_date") \
              .option("user", db_user)\
              .option("password", db_pass) \
              .mode("append") \
              .save()
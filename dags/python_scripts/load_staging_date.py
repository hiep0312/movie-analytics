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

def load_staging_date(spark, params):
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

    s3_bucket = params["s3_bucket"]
    s3_key = params["s3_key"]

    movies_df = spark.read.option("header", "true") \
                           .csv(f"s3a://{s3_bucket}/{s3_key}/movies_metadata.csv", 
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
        .format("jdbc") \
        .option("url", params['postgres_url']) \
        .option("dbtable", "movies.stage_date") \
        .option("user", params['db_user']) \
        .option("password", params['db_pass']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


def format_datetime(ts):
    return datetime.fromtimestamp(ts/1000.0) 

    
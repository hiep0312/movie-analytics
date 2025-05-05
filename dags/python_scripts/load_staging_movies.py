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

def load_staging_movies(spark, params):
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
        Fld("production_companies", String()),
        Fld("production_countries",  String()),
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

    movies_df = movies_df.select(
        col("id").alias("movie_id"),
        col("adult").alias("is_adult"),
        col("budget"),
        col("original_language"),
        col("title"),
        col("popularity"),
        col("release_date"),
        col("revenue"),
        col("vote_count"),
        col("vote_average")
    )

    movies_df = movies_df.na.drop()

    movies_df = movies_df.dropDuplicates(['movie_id'])

    movies_df.write \
        .format("jdbc") \
        .option("url", params['postgres_url']) \
        .option("dbtable", "movies.stage_movies") \
        .option("user", params['db_user']) \
        .option("password", params['db_pass']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()



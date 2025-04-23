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
from pyspark.sql.functions import (explode, from_json)


def load_staging_genre(spark, params):
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
    
    genre_schema = Array(StructType([Fld("id", Int()), Fld("name", String())]))

    movies_df = movies_df.withColumn("genres", explode(from_json("genres", genre_schema))) \
                         .withColumn("genre_id", col("genres.id")) \
                         .withColumn("genre_name", col("genres.name")) \
    
    movie_genre = movies_df.select("id", "genre_id").distinct()
    movie_genre = movie_genre.select(col("id").alias("movie_id"), col("genre_id"))
    
    genre = movies_df.select("genre_id", "genre_name").distinct()
    genre = genre.na.drop()

    genre.write \
            .format("jdbc")  \
            .option("url", params['postgres_url']) \
            .option("dbtable", "movies.stage_date") \
            .option("user", params['db_user'])\
            .option("password", params['db_pass']) \
            .mode("append") \
            .save()
    
    movie_genre.write \
            .format("jdbc")  \
            .option("url", params['postgres_url']) \
            .option("dbtable", "movies.stage_movie_genre") \
            .option("user", params['db_user']) \
            .option("password", params['db_pass']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    
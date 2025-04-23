import sys
import os
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, TimestampType as Timestamp, 
                               DateType as Date, LongType as Long)
from pyspark.sql.functions import col

def load_staging_rating(spark, params):
    ratings_schema = StructType([
        Fld("userId", Int()),
        Fld("movieId", Int()),
        Fld("rating", Dbl()),
        Fld("timestamp", Long())
    ])

    s3_bucket = params["s3_bucket"]
    s3_key = params["s3_key"]

    ratings_df = spark.read.option("header", "true") \
                           .csv(f"s3a://{s3_bucket}/{s3_key}/ratings.csv", schema=ratings_schema)
    
    ratings_df = ratings_df.select(
        col("userId").cast("int").alias("user_id"),
        col("movieId").cast("int").alias("movie_id"),
        col("rating").cast("double")
    )

    ratings_df.show(5)
    ratings_df.printSchema()

    ratings_df.write \
        .format("jdbc") \
        .option("url", params['postgres_url']) \
        .option("dbtable", "movies.stage_ratings") \
        .option("user", params['db_user']) \
        .option("password", params['db_pass']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    

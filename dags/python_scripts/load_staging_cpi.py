import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, TimestampType as Timestamp, 
                               DateType as Date, FloatType as Float)
from pyspark.sql.functions import col

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
    
if __name__ == "__main__":
    minio_bucket = sys.argv[1]
    minio_folder = sys.argv[2]
    minio_key = sys.argv[3]
    minio_secret_key = sys.argv[4]
    postgres_conn_string = sys.argv[5]
    db_user = sys.argv[6]
    db_pass = sys.argv[7]

    spark = create_spark_session(minio_key=minio_key,minio_secret_key=minio_secret_key)

    cpi_schema = StructType([
        Fld("DATE", Date()),
        Fld("CUSR0000SS62031", Float())
    ])

    cpi_df = spark.read.option("header", "true") \
                .csv("s3a://{}/{}/consumer_price_index.csv".format(minio_bucket, minio_folder), 
                                schema=cpi_schema)
    
    cpi_df = cpi_df.select(
        col("DATE").alias("date_cd"),
        col("CUSR0000SS62031").alias("consumer_price_index")
    )

    cpi_df = cpi_df.filter(cpi_df.date_cd.isNotNull())

    cpi_df.write \
              .format("jdbc")  \
              .option("url", postgres_conn_string) \
              .option("dbtable", "movies.stage_cpi") \
              .option("user", db_user)\
              .option("password", db_pass) \
              .mode("append") \
              .save()
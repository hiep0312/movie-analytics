import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, TimestampType as Timestamp, 
                               DateType as Date, FloatType as Float)
from pyspark.sql.functions import col

def load_staging_cpi(spark, params):
    cpi_schema = StructType([
        Fld("DATE", Date()),
        Fld("CUSR0000SS62031", Float())
    ])

    s3_bucket = params["s3_bucket"]
    s3_key = params["s3_key"]

    cpi_df = spark.read.option("header", "true") \
                .csv(f"s3a://{s3_bucket}/{s3_key}/consumer_price_index.csv", 
                                schema=cpi_schema)
    
    cpi_df = cpi_df.select(
        col("DATE").alias("date_cd"),
        col("CUSR0000SS62031").alias("consumer_price_index")
    )

    cpi_df = cpi_df.filter(cpi_df.date_cd.isNotNull())

    cpi_df.write \
        .format("jdbc") \
        .option("url", params['postgres_url']) \
        .option("dbtable", "movies.stage_cpi") \
        .option("user", params['db_user']) \
        .option("password", params['db_pass']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    
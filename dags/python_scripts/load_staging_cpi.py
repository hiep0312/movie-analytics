import os
import sys
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DataType as Date, FloatType as Float)
from pyspark.sql.functions import col

def create_spark_session(minio_key, minio_access_key):
    pass
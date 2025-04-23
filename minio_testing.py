from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Read from S3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.access.key", aws_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Đọc file từ S3
df = spark.read.option("header", "true").csv("s3a://my-movies-analytics/the-movies-dataset/ratings.csv")
df.show(5)

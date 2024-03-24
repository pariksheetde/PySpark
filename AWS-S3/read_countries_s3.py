from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
# import boto3

if __name__ == "__main__":
    print("Extract Data from S3")

access_key = "AKIAT3T4LD6GT5EPUSVL"
secret_key = "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"

# commenting for git

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.1,com.amazonaws:aws-java-sdk:1.12.10") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.access.key", access_key) \
    .config("fs.s3a.secret.key", secret_key) \
    .appName("Extract Data from S3").master("local[3]").getOrCreate()

# .config("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
# .config("com.amazonaws.services.s3.enableV4", "true")

df = spark.read.option("header", "true").csv("s3a://data-lake-hr/spark-dataset/countries.csv")

df.show(10,truncate = False)
spark.stop()
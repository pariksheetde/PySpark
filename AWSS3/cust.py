from pyspark.sql import SparkSession
from AWSS3 import cust

aws_key = "YOUR_ACCESS_KEY"
aws_secret = "YOUR_SECRET_KEY"

spark = SparkSession.builder.getOrCreate()
df = cust.read_s3_csv_from_bucket(
    spark,
    bucket='my-bucket',
    folder='sales',
    filename='cust.csv',
    aws_access_key=aws_key,
    aws_secret_key=aws_secret,
    options={'header':'true','inferSchema':'true'}
)
df.show(5)
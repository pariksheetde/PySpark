from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : BigData, Script : Amazon Books Analysis 1")

spark = SparkSession.builder.appName("Amazon Books Analysis 1").master("local[3]").getOrCreate()

# define schema
books_schema = StructType([
    StructField("Name", StringType(),True),
    StructField("Author", StringType()),
    StructField("User_Rating", DoubleType()),
    StructField("Reviews", IntegerType(), True),
    StructField("Price", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Genre", StringType(), True)
])

books_df = spark.read \
    .format("csv") \
    .schema(books_schema) \
    .option("header", "true") \
    .option("mode", "failFast") \
    .option("sep", ",") \
    .option("nullValue", "") \
    .option("compression", "snappy") \
    .option("path","D:/DataSet/DataSet/SparkDataSet/amazon_books.csv") \
    .load()

agg_price = books_df.select("Genre", "Price") \
    .groupBy("Genre") \
    .agg(
        max("Price").alias("Max_Price"),
        mean("Price").alias("Min_Price"),
        avg("Price").alias("Avg_Price"),
        count("*").alias("Count")
) \
    .sort(desc("Max_Price"))

agg_price.show(10, False)
spark.stop()


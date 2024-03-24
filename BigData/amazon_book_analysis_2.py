from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : BigData, Script : Amazon Books Analysis 2")

spark = SparkSession.builder.appName("Amazon Books Analysis 2").master("local[3]").getOrCreate()

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

sel_books = books_df.select("User_Rating", "Author", "Year") \
      .groupBy("Year") \
      .agg(
        round(sum("User_Rating"),2).alias("Sum_Ratings"),
        count("Author").alias("Authors_Cnt")
        ) \
    .orderBy(asc("Year"))

sel_books.printSchema()
sel_books.show(10, False)

spark.stop()
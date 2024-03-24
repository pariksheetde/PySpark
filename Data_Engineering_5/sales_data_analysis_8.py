from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 8")

spark = SparkSession.builder.appName("Sales Data Analysis 8").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

print("COMPUTE MAX SALES IN EACH COUNTRY FOR EACH PRODUCT")
country_df = sales_df.selectExpr("COUNTRY as Country", "PRODUCTLINE as Product", "Sales as Sales") \
      .groupBy("Country", "Product") \
      .agg(
        sum("Sales").alias("Sales")
      ).orderBy(desc("Sales"))
country_df.show(10, False)

top_sales_country = country_df.selectExpr("Country", "Product", "Sales") \
      .withColumn("Rank", rank().over(Window.partitionBy("Country").orderBy(desc("Sales"))))
top_sales_country.show(10, False)

spark.stop()
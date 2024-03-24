from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 9")

spark = SparkSession.builder.appName("Sales Data Analysis 9").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

sales_df.printSchema()
sales_df.show(10, False)

print("COMPUTE MAX SALES IN EACH COUNTRY FOR EACH PRODUCT IN EACH YEAR")
country_df = sales_df.selectExpr("COUNTRY as Country", "PRODUCTLINE as Product", "YEAR_ID as Year", "SALES as Sales") \
      .groupBy("Country", "Year", "Product") \
      .agg(
        sum("Sales").alias("Sales")
      ).orderBy(asc("Year"), desc("Sales"))
country_df.show(10, False)

top_sales = country_df.select("Country", "Year", "Product", "Sales") \
      .withColumn("Rank", rank().over(Window.partitionBy("Country", "Year").orderBy(desc("Sales"))))
top_sales.show(10, False)
    
spark.stop()
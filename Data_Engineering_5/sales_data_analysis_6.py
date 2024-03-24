from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 6")

spark = SparkSession.builder.appName("Sales Data Analysis 6").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

print("CALCULATE THE SALES FOR EACH DEALSIZE IN EACH YEAR")
deal_df = sales_df.selectExpr("DEALSIZE as Deal",  "STATUS as Status", "YEAR_ID as Year","SALES as Sales") \
      .groupBy("Year", "Status", "Deal") \
      .agg(
        sum("Sales").alias("Sum_Sales")
      ).sort(asc("Year"), desc("Sum_Sales"))
deal_df.show(10, False)

print("COMPUTE THE DEAL FOR EACH STATUS SIZE FOR THE YEAR 2003")
top_deal_2003 = deal_df.selectExpr("Year", "Status", "Deal", "Sum_Sales") \
      .withColumn("Rank", rank().over(Window.partitionBy("Year", "Status").orderBy(desc("Sum_Sales")))) \
      .filter("Year == 2003")
top_deal_2003.show(10, False)

print("COMPUTE THE DEAL FOR EACH STATUS SIZE FOR THE YEAR 2004")
top_deal_2004 = deal_df.selectExpr("Year", "Status", "Deal", "Sum_Sales") \
      .withColumn("Rank", rank().over(Window.partitionBy("Year", "Status").orderBy(desc("Sum_Sales")))) \
      .filter("Year == 2004")
top_deal_2004.show(10, False)

spark.stop()
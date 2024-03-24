from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 7")

spark = SparkSession.builder.appName("Sales Data Analysis 7").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

print("COMPUTE SALES FOR EACH ORDER NUMBER")
qty_df = sales_df.selectExpr("YEAR_ID as Year", "ORDERNUMBER as Order_NO", "SALES as Sales") \
      .groupBy("Year", "Order_NO") \
      .agg(
        sum("Sales").alias("Sales")
      ).orderBy(asc("Year"), desc("Sales")) \
      .where("Year in (2003, 2004, 2005)")
qty_df.show(200, False)

top_selling_qty = qty_df.selectExpr("Year", "Order_NO", "Sales") \
      .withColumn("Rank", rank().over(Window.partitionBy("Year").orderBy(desc("Sales")))) \
      .where("Rank = 1")
top_selling_qty.show(100, False)

spark.stop()
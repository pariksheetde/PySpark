from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 5")

spark = SparkSession.builder.appName("Sales Data Analysis 5").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

print("COMPUTE THE DEAL SIZE THAT HAPPENED IN EACH YEAR")
deal_df = sales_df.selectExpr("DEALSIZE as Deal", "YEAR_ID as Year") \
      .groupBy("Year","Deal") \
      .agg(
        count("Deal").alias("Deal_Cnt") \
      ).sort(desc_nulls_last("Deal_Cnt"))
deal_df.show(10, False)

print(f"Top Deal")
top_deal = deal_df.select("Year", "Deal", "Deal_Cnt") \
      .withColumn("Rank", rank().over(Window.partitionBy("Year").orderBy(desc("Deal_Cnt"))))
top_deal.show(10, False)

spark.stop()
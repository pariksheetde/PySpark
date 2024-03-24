from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script : IPL Analysis 1")

spark = SparkSession.builder.appName("IPL Analysis 1").master("local[3]").getOrCreate()


ipl_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv"))

print(f"count number of IPL matches played")
ipl_df.show(10, False)
print(f"Number of IPL matches played: {ipl_df.count()}")

print(f"count number of matches played in which city and stadium")

city_venue_cnt = ipl_df.select("city", "venue") \
    .groupBy("city", "venue") \
    .agg(
      count("city").alias("City_Cnt")
    ).sort(col("City_Cnt").desc_nulls_last())

city_venue_cnt.show(10, False)

print(f"Which team has won most number of games")
max_win_cnt = ipl_df.select("winner") \
      .groupBy("winner") \
      .agg(
        count("winner").alias("maximum_win")
      ).sort(desc(col("maximum_win")))
max_win_cnt.show(10, False)

spark.stop()
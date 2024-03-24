from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd

if __name__ == "__main__":
    print("Package : Bank, Script : Flight_Data_Analysis_1")

spark = SparkSession.builder.appName("Flight_Data_Analysis_1").master("local[3]").getOrCreate()

# spark.sql.shuffle.partitions configures the number of partitions
# that are used when shuffling data for joins or aggregations.

spark.conf.set("spark.sql.shuffle.partitions", 100)
spark.conf.set("spark.default.parallelism", 100)

flight_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("D:/DataSet/DataSet/SparkDataSet/flight.csv")

flight_part = flight_df.repartition(10)
print(f"Number of Partitions {flight_part.rdd.getNumPartitions()}")

flight_kol = flight_part.where("Source == 'Kolkata'")
flight_kol_sel = flight_kol.select("Airline", "Date_Of_Journey", "Source", "Destination", "Route", "Duration")
flight_kol_cnt = flight_kol_sel.select("Airline", "Date_Of_Journey", "Source", "Destination", "Route", "Duration") \
      .groupBy("Destination") \
      .count().show()

spark.stop()
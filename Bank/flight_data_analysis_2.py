from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    print("Package : Bank, Script : Flight_Data_Analysis_2")

spark = SparkSession.builder.appName("Flight_Data_Analysis_2").master("local[3]").getOrCreate()

flight_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("D:/DataSet/DataSet/SparkDataSet/flight.csv")

flight_part = flight_df.repartition(10)
print(f"Number of Partitions : {flight_part.rdd.getNumPartitions()}")

# SQL_tab = flight_part.createOrReplaceTempView("flight_analysis")
# SQL_qry = spark.sql("""select * from flight_analysis where Source = "Kolkata" and Destination = "Delhi"
# """)

final_flight_df = flight_df.select("Airline", "Date_of_Journey", "Source", "Destination", "Route",
                                   "Dep_Time", "Arrival_Time", "Duration", "Total_Stops", "Additional_Info") \
    .withColumn("Altered_Route", regexp_replace("Route", "[^0 9A-Za z]", "-")) \
        .drop("Route")

final_flight_df.show(10, False)

spark.stop()
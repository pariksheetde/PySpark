from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
import sys
sys.stdout.reconfigure(encoding='utf-8')


def process_fifa_stats():
    fifa_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("inferSchema", "true") \
    .option("nullValue", "NA") \
    .option("sep", ",") \
    .option("compression", "snappy") \
    .option("dateFormat", "dd/MM/yyy") \
    .load("D:/DataSet/DataSet/SparkDataSet/FIFA_Stats.csv")

    sel_fifa = fifa_df.selectExpr("Year", "Datetime", "Stage as RoundRobin", "City", "Home_Team_Name", "Home_Team_Goals",
      "Away_Team_Goals", "Away_Team_Name",
      "Half_Time_Home_Goals as 1st_Half_Home_Goals", "Half_Time_Away_Goals as 1st_Half_Away_Goals",
      "Home_Team_Goals - Half_Time_Home_Goals as 2nd_Half_Home_Goals",
      "Away_Team_Goals - Half_Time_Away_Goals as 2nd_Half_Away_Goals") \
      .withColumn("Goals", col("Home_Team_Goals") + col( "Away_Team_Goals"))

    all_fifa_cln_up = sel_fifa.selectExpr("Year", "Datetime", "RoundRobin", "City", "Home_Team_Name", "Away_Team_Name", "Home_Team_Goals",
      "Away_Team_Goals", "1st_Half_Home_Goals", "2nd_Half_Home_Goals", "1st_Half_Away_Goals", "2nd_Half_Away_Goals", "Goals") \
      .where("Year <= 2000 and RoundRobin like 'Group%'")

    return all_fifa_cln_up

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Data_Frame_2") \
        .master("local[3]") \
        .getOrCreate()

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")        


    transformed_df = process_fifa_stats()
    transformed_df.show(transformed_df.count(), truncate=False)
    print(f"Total Records Processed: {transformed_df.count()}")

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR") 

    # Stop Spark session
    spark.stop()
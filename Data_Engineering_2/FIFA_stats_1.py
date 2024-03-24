from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_2, Script : FIFA World Cup Analysis 1")

spark = SparkSession.builder.appName("FIFA World Cup Analysis 1").master("local[3]").getOrCreate()

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
      .filter(col("Year") <= 2000) \
      .filter("RoundRobin like 'Group%'")

# all_fifa_cln_up.printSchema()
all_fifa_cln_up.show(10, False)
print(f"Rows effected: {all_fifa_cln_up.count()}")
spark.stop()
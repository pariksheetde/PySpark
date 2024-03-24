from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: IPL Analysis 2")

spark = SparkSession.builder.appName("IPL Analysis 2").master("local[3]").getOrCreate()


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

print(f"Matches played by each host team")
host_played_each_team = ipl_df.select("team1") \
    .groupBy("team1") \
    .agg(
      count("team1").alias("Matches_Played_by_each_host_team")
    ).sort(desc("Matches_Played_by_each_host_team"))

host_played_each_team.show(10, False)

print(f"Matches played by each guest team")
guest_played_each_team = ipl_df.select("team2") \
    .groupBy("team2") \
    .agg(
      count("team2").alias("Matches_Played_by_each_guest_team")
    ).sort(desc("Matches_Played_by_each_guest_team"))

guest_played_each_team.show(10, False)

print(f"Calculate number of matches played by each team")
matches_played_each_team = host_played_each_team.union(guest_played_each_team) \
      .groupBy("team1") \
      .agg(
        sum("Matches_Played_by_each_host_team").alias("Matches_Played")
      ).orderBy(desc("Matches_Played"))

matches = matches_played_each_team.selectExpr("team1 as Team", "Matches_Played")
matches.show(10, truncate = False)

print("Which team has won most number of games")
max_win_cnt = ipl_df.select("winner") \
    .groupBy("winner") \
    .agg(
        count("winner").alias("win_cnt")
).sort(desc("win_cnt")) \
    .show(10, False)

spark.stop()
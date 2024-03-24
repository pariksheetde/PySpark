from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: IPL Analysis 7")

spark = SparkSession.builder.appName("IPL Analysis 7").master("local[3]").getOrCreate()

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

print("COMPUTE WINNING PERCENTAGE FOR HOME & AWAY TEAM")
print("MATCHES PLAYED BY HOME TEAM")
home_team = (ipl_df.selectExpr("team1 as Home_Team")
    .groupBy("Home_Team")
    .agg(
        count("Home_Team").alias("Home_Matches_Played")
).sort(desc(col("Home_Matches_Played"))))
home_team.show(10, False)

print("MATCHES PLAYED BY AWAY TEAM")
away_team = (ipl_df.selectExpr("team2 as Away_Team")
    .groupBy("Away_Team")
    .agg(
        count("Away_Team").alias("Away_Matches_Played")
    ).sort(desc(col("Away_Matches_Played"))))
away_team.show(10, False)

print("MATCHES WON BY HOME TEAM")
home_win = (ipl_df.selectExpr("team1 as Host_Team", "winner")
    .groupBy("Host_Team", "winner")
    .agg(
        count("winner").alias("Host_Team_Won")
    ).filter("Host_Team = winner"))
home_win.show(10, False)

print("MATCHES WON BY VISITING TEAM")
away_win = (ipl_df.selectExpr("team2 as Guest_Team", "winner")
    .groupBy("Guest_Team", "winner")
    .agg(
        count("winner").alias("Guest_Team_Won")
    ).filter("Guest_Team = winner"))
away_win.show(10, False)

spark.stop()
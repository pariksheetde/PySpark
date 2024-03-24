from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: IPL Analysis 4")

spark = SparkSession.builder.appName("IPL Analysis 4").master("local[3]").getOrCreate()

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

print("CALCULATE THE COUNT AND WINNING PERCENTAGE OF MATCHES WHEN HOME TEAM WINS")
home_team = (ipl_df.selectExpr("team1 as Home_Team")
      .groupBy("Home_Team")
      .agg(
        count("Home_Team").alias("Matches_Played")
      ).sort(desc("Matches_Played")))
home_team.show(10, False)

winner = (ipl_df.selectExpr("winner as Team", "team1 as Host_Team")
      .groupBy("Team", "Host_Team")
      .agg(
        count("Team").alias("Matches_Won")
      )
      .where("Team = Host_Team")
      .drop("Team")
      .sort(desc("Matches_Won")))
winner.show(10, False)

join_expr = home_team.Home_Team == winner.Host_Team
home_team_win_pct = home_team.join(winner, join_expr, "inner") \
        .withColumn("Win%", (col("Matches_Won") / col("Matches_Played")) * 100) \
        .drop("Host_Team") \
        .sort(desc(col("Win%")))
home_team_win_pct.show(10, False)

spark.stop()
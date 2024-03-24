from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: IPL Analysis 5")

spark = SparkSession.builder.appName("IPL Analysis 5").master("local[3]").getOrCreate()

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

print("CALCULATE THE COUNT AND WINNING PERCENTAGE OF MATCHES WHEN AWAY TEAM WINS")
away_team = (ipl_df.selectExpr("team2 as Away_Team")
.groupBy("Away_Team")
.agg(
    count("Away_Team").alias("Matches_Played")
)
.sort(desc("Matches_Played")))
away_team.show(10, False)

away_team_win = (ipl_df.selectExpr("team2 as Away_Team", "winner as Winning_Team")
      .groupBy("Away_Team", "Winning_Team")
      .agg(
        count("Away_Team").alias("Matches_Won")
      )
      .filter("Away_Team = Winning_Team")
      .drop("Away_Team")
      .sort(desc("Matches_Won")))
away_team_win.show(10, False)

join_expr = away_team.Away_Team == away_team_win.Winning_Team
away_team_win_pct = away_team.join(away_team_win, join_expr, "inner")

away_team_win_pct.selectExpr("Away_Team", "Matches_Played", "Matches_Won") \
.withColumn("Win%", (col("Matches_Won") / col("Matches_Played")) * 100) \
.show(10, False)

spark.stop()
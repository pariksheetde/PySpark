from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: IPL Analysis 6")

spark = SparkSession.builder.appName("IPL Analysis 6").master("local[3]").getOrCreate()

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

print("CALCULATE MATCHES PLAYED FOR HOME AND AWAY TEAM")
print("Home Team winning records")

home_team = (ipl_df.selectExpr("team1 as Home_Team")
    .groupBy("Home_Team")
    .agg(
        count("Home_Team").alias("Home_Matches_Played")
).sort(desc("Home_Matches_Played")))
home_team.show(10, False)

print("Away Team winning records")
away_team = (ipl_df.selectExpr("team2 as Away_Team")
    .groupBy("Away_Team")
    .agg(
        count("Away_Team").alias("Away_Matches_Played")
).sort(desc("Away_Matches_Played")))
away_team.show(10, False)


join_expr = home_team.Home_Team == away_team.Away_Team
winning_pct = home_team.join(away_team, join_expr, "inner") \
    .selectExpr("Home_Team as Team", "Home_Matches_Played", "Away_Matches_Played") \
    .withColumn("Total_Matches_Played", (col("Home_Matches_Played") + col("Away_Matches_Played"))) \
    .drop("Home_Team") \
    .sort(desc(col("Total_Matches_Played")))

winning_pct.show(10, False)

spark.stop()
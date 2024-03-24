from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: IPL Analysis 3")

spark = SparkSession.builder.appName("IPL Analysis 3").master("local[3]").getOrCreate()

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

print("Matches played by each host team")
host_played_each_team = ipl_df.select("team1") \
    .groupBy("team1") \
    .agg(
      count("team1").alias("Matches_Played_by_each_host_team")
    ).sort(desc("Matches_Played_by_each_host_team"))
host_played_each_team.show(10, False)

print("Matches played by each guest team")
guest_played_each_team = ipl_df.select("team2") \
    .groupBy("team2") \
    .agg(
      count("team2").alias("Matches_Played_by_each_guest_team")
    ).sort(desc("Matches_Played_by_each_guest_team"))
guest_played_each_team.show(10, False)

print("Calculate number of matches played by each team")
matches_played_each_team = host_played_each_team.union(guest_played_each_team) \
    .groupBy("team1") \
    .agg(
      sum("Matches_Played_by_each_host_team").alias("Matches_Played")
    ) \
    .orderBy(desc("Matches_Played"))
matches_played_each_team.show(10, False)

matches = matches_played_each_team.selectExpr("team1 as Team", "Matches_Played")
matches.show(10, False)

print("Which team has won most number of games")
max_win_cnt = ipl_df.selectExpr("winner as Team") \
.groupBy("Team") \
.agg(
    count("Team").alias("Win_Cnt")
).sort(desc("Win_Cnt"))
max_win_cnt.show(10, False)

# rename department_id in cln_emp_df to dept_id
cln_match = matches.withColumnRenamed("Team", "Club")
join_expr = cln_match.Club == max_win_cnt.Team

# write the inner join condition between cln_dept_df & cln_emp_dept_id
match_results = cln_match.join(max_win_cnt, join_expr, "inner") \
      .selectExpr("Team", "Matches_Played", "Win_Cnt") \
      .orderBy(desc("Matches_Played"))
match_results.show(10, False)

match_results.selectExpr("Team", "Matches_Played as Matches", "Win_Cnt as Win") \
      .withColumn("Win_%", (col("Win") / col("Matches"))*100) \
      .sort(desc("Win_%")) \
      .show(10, False)

spark.stop()
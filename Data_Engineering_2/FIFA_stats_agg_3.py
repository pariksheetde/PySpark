from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

def process_fifa_stats_agg(spark):
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

    select_fifa_df = fifa_df.selectExpr("Year", "Datetime", "Stage as RoundRobin", "City", "Home_Team_Name", "Home_Team_Goals",
        "Away_Team_Goals", "Away_Team_Name",
        "Half_Time_Home_Goals as 1st_Half_Home_Goals",
        "Half_Time_Away_Goals as 1st_Half_Away_Goals",
        "Home_Team_Goals - Half_Time_Home_Goals as 2nd_Half_Home_Goals",
        "Away_Team_Goals - Half_Time_Away_Goals as 2nd_Half_Away_Goals") \
        .withColumn("Goals", col("Home_Team_Goals") + col( "Away_Team_Goals")) \
        .sort(col("Year").asc(),col("Home_Team_Name").asc())

    agg_goals_year_df = select_fifa_df.groupBy("Year", "Home_Team_Name") \
    .agg(
            sum("Goals").alias("Sum_Goals"),
            round(mean("Goals"),4).alias("Avg_Goals"),
            count("Goals").alias("Cnt_Goals"),
            min("Goals").alias("Min_Goals")
        ) \
    .sort(col("Sum_Goals").desc())

    return select_fifa_df

def Goals_Aggregation(spark, select_fifa_df):
    sql_agg_temp_df = select_fifa_df.createOrReplaceTempView("Goals_Aggregation_temp_VW")
    agg_goals_year_df = spark.sql(
    """select * from (select
      Year, Home_Team_Name,
      sum(goals) over (partition by Home_Team_Name, Year order by Goals desc) as Sum_Goals,
      dense_rank() over (partition by Home_Team_Name, Year order by Goals asc) as DRank
      from
      Goals_Aggregation_temp_VW
      order by Year asc, Sum_Goals desc) as Temp where DRank = 1""")

    return agg_goals_year_df

def Top_Goals_Scorer(spark, agg_goals_year_df):
    sql_top_goal_scorer_temp_df = agg_goals_year_df.createOrReplaceTempView("Top_Goals_Scorer_temp_VW")
    top_goals_scorer_df = spark.sql(
    """select Year, Home_Team_Name, Sum_Goals,
      dense_rank(Sum_Goals) over (partition by Year order by Year) as DRank,
      rank(Sum_Goals) over (partition by Year order by Year) as Rank
      from Top_Goals_Scorer_temp_VW
      order by Year, Sum_Goals desc""")

    return top_goals_scorer_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("FIFA Stats Agg 3").master("local[3]").getOrCreate()

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    result_df = process_fifa_stats_agg(spark)
    # result_df.show(100, False)
    # print(f"Total Records Processed: {result_df.count()}")

    # Process Goals Aggregation
    goals_agg_df = Goals_Aggregation(spark, result_df)
    goals_agg_df.show(100, False)
    print(f"Total Records Processed in Goals Aggregation: {goals_agg_df.count()}")

    # Process Top Goals Scorer
    top_goals_scorer_df = Top_Goals_Scorer(spark, goals_agg_df)
    top_goals_scorer_df.show(100, False)
    print(f"Total Records Processed in Top Goals Scorer: {top_goals_scorer_df.count()}")

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")
    
    # Stop Spark session
    spark.stop()
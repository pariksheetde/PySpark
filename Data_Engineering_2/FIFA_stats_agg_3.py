from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_2, Script : FIFA World Cup Aggregation Analysis 3")

spark = SparkSession.builder.appName("FIFA World Cup Aggregation Analysis 3").master("local[3]").getOrCreate()

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
    .withColumn("Goals", col("Home_Team_Goals") + col( "Away_Team_Goals")) \
    .sort(col("Year").asc(),col("Home_Team_Name").asc())

sel_fifa.printSchema()
sel_fifa.show()
print(f"Records Effected: {sel_fifa.count()}")

agg_goals_year = sel_fifa.groupBy("Year", "Home_Team_Name") \
    .agg(
            sum("Goals").alias("Sum_Goals"),
            round(mean("Goals"),4).alias("Avg_Goals"),
            count("Goals").alias("Cnt_Goals"),
            min("Goals").alias("Min_Goals")
        ) \
    .sort(col("Sum_Goals").desc())

agg_goals_year.show(truncate = False)

sel_agg_cols = sel_fifa.select("Year", "Home_Team_Name", "Goals")
sel_agg_cols.show(truncate = False)

sql_df = sel_agg_cols.createOrReplaceTempView("Goals_Aggregation")
qry_df = spark.sql(
    """select * from (select
      Year, Home_Team_Name,
      sum(goals) over (partition by Home_Team_Name, Year order by Goals desc) as Sum_Goals,
      dense_rank() over (partition by Home_Team_Name, Year order by Goals asc) as DRank
      from
      Goals_Aggregation
      order by Year asc, Sum_Goals desc) as Temp where DRank = 1""")

qry_df.show(truncate = False)
print(f"Records Effected: {qry_df.count()}")

top_goals = qry_df.createOrReplaceTempView("Top_Goals_Scorer")
qry_df_agg = spark.sql(
    """
      select Year, Home_Team_Name, Sum_Goals,
      dense_rank(Sum_Goals) over (partition by Year order by Year) as DRank,
      rank(Sum_Goals) over (partition by Year order by Year) as Rank
      from Top_Goals_Scorer
      order by Year, Sum_Goals desc
      """)

qry_df_agg.show(truncate = False)
print(f"Records Effected: {qry_df_agg.count()}")
spark.stop()
from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : Indian Premier League Analysis 3")

spark = SparkSession.builder.appName("Indian Premier League Analysis 3").master("local[3]").getOrCreate()

ipl_schema = StructType([
    StructField("id", IntegerType()),
    StructField("city", StringType()),
    StructField("schedule", StringType()),
    StructField("player_of_match", StringType()),
    StructField("venue", StringType()),
    StructField("neutral_venue", StringType()),
    StructField("team1", StringType()),
    StructField("team2", StringType()),
    StructField("toss_winner", StringType()),
    StructField("toss_decision", StringType()),
    StructField("winner", StringType()),
    StructField("result", StringType()),
    StructField("result_margin", StringType()),
    StructField("eliminator", StringType()),
    StructField("method", StringType()),
    StructField("umpire1", StringType()),
    StructField("umpire2", StringType()),
  ])

ipl_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(ipl_schema) \
    .option("dateFormat", "dd-MM-yyyy") \
    .option("sep", ",") \
    .option("nullValue", "") \
    .option("compression", "snappy") \
    .option("path","D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv") \
    .load()

bat_bowl_df = ipl_df.select(col("*"), expr("case when result = 'runs' then 'batting_first' "
                                           "when result = 'wicket' then 'bowling_first' else 'NoPlay' end").alias("bat_bowl")) \
    .withColumnRenamed("bat_bowl", "batting_fielding")

filtered_ipl_df = (bat_bowl_df.where("winner like 'Kolkata%'")
      .filter("result = 'runs'")
      .filter("city != 'Kolkata'"))

filtered_ipl_df.show(10, truncate = False)
print(f"Records Effected : {filtered_ipl_df.count()}")


spark.stop()
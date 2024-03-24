from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : Indian Premier League Analysis 1")

spark = SparkSession.builder.appName("Indian Premier League Analysis 1").master("local[3]").getOrCreate()

ipl_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("schedule", StringType(), True),
    StructField("player_of_match", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("neutral_venue", StringType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("toss_decision", StringType(), True),
    StructField("winner", StringType(), True),
    StructField("result", StringType(), True),
    StructField("result_margin", StringType(), True),
    StructField("eliminator", StringType(), True),
    StructField("method", StringType(), True),
    StructField("umpire1", StringType(), True),
    StructField("umpire2", StringType(), True)
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

ipl_df.show(10, truncate = False)
ipl_df.printSchema()

(ipl_df.write
      .option("delimiter", "\t")
      .option("header", "true")
      .mode("Overwrite")
      .csv("D:/DataSet/OutputDataset/IPL"))

print(f"Records Effected {ipl_df.count()}")

spark.stop()
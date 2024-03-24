from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    print("Package : Bank, Script : Indian Premier League DataSet API")

spark = SparkSession.builder.appName("Indian Premier League DataSet API").master("local[3]").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 100)
spark.conf.set("spark.default.parallelism", 100)

emp_schema = StructType([
    StructField("id", IntegerType()),
    StructField("city", StringType()),
    StructField("schedule", StringType()),
    StructField("player_of_match", StringType()),
    StructField("venue", StringType()),
    StructField("neutral_venue", StringType()),
    StructField("team1", StringType()),
    StructField("team2", IntegerType()),
    StructField("toss_winner", FloatType()),
    StructField("toss_decision", IntegerType()),
    StructField("winner", IntegerType()),
    StructField("result", StringType()),
    StructField("result_margin", StringType()),
    StructField("eliminator", IntegerType()),
    StructField("method", FloatType()),
    StructField("umpire1", IntegerType()),
    StructField("umpire2", IntegerType()),

]
)


ipl_df  = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dateFormat", "M/D/Y") \
    .option("mode", "FAILFAST") \
    .csv("D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv")

ipl_repartition_df = ipl_df.repartition(5)

ipl_sel_df = ipl_repartition_df.where("city = 'Kolkata'") \
    .select("city", "schedule", expr("player_of_match as MOM"), "venue", "team1", "team2", "winner")

ipl_sel_df.show(10, False)
print(f"Number of Records Effected {ipl_sel_df.count()}")

spark.stop()
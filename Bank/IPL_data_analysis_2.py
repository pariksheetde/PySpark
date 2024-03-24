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

ipl_df  = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dateFormat", "M/D/Y") \
    .option("mode", "FAILFAST") \
    .csv("D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv")

ipl_sel_df = ipl_df.selectExpr("city", "schedule", "winner",
                               "venue as stadium", "team1", "team2", "result", "neutral_venue")
ipl_sel_df.show(20, truncate = False)
ipl_sel_df.printSchema()

spark.stop()
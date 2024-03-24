from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : Video_Games_Analysis_1")

spark = SparkSession.builder.appName("Video_Games_Analysis_1").master("local[3]").getOrCreate()

vg_schema = StructType([
    StructField("Rank",IntegerType(),True),
    StructField("Name",StringType(),True),
    StructField("Platform",StringType(),True),
    StructField("Year", IntegerType(), True),
    StructField("Genre", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("NA_Sales", FloatType(), True),
    StructField("EU_Sales", FloatType(), True),
    StructField("JP_Sales", FloatType(), True),
    StructField("Other_Sales", FloatType(), True),
    StructField("Global_Sales", FloatType(), True)
  ])

games_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(vg_schema) \
    .option("mode", "PERMISSIVE") \
    .option("sep", ",") \
    .option("nullValue", "") \
    .option("compression", "snappy") \
    .option("path","D:/DataSet/DataSet/SparkDataSet/vgsales.csv") \
    .load()

# games_df.show()

ms_agg = games_df.select("Publisher", "NA_Sales") \
      .groupBy("Publisher") \
      .agg(
        mean("NA_Sales").alias("Avg_NA_sales"),
        stddev("NA_Sales").alias("Std_NA_Sales")
       ) \
      .filter("Publisher in ('Microsoft Game Studios', 'Nintendo')") \
      .orderBy("Avg_NA_sales") \

ms_agg.show(truncate = False)

spark.stop()
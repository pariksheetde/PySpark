from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : Video_Games_Analysis_2")

spark = SparkSession.builder.appName("Video_Games_Analysis_2").master("local[3]").getOrCreate()

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

total_sales_df = games_df.select(
            (col("NA_Sales") + col("EU_Sales") + col("JP_Sales") +
        col("Other_Sales") + col("Global_Sales")).alias("Total_Sales"), "Name")

total_sales_df.show(truncate = False)
print(f"Records Effected {total_sales_df.count()}")

spark.stop()
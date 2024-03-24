from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Cars Data Analysis 2")

spark = SparkSession.builder.appName("Cars Data Analysis 2").master("local[3]").getOrCreate()

cars_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/cars.json"))

cars_df.show(truncate=False)
print(f"Number of records in the cars_df DF: {cars_df.count()}")

print(f"Number of Origins")
origin_cnt = cars_df.select(col("Origin")) \
    .groupBy(col("Origin")) \
    .agg(
      count("Origin")
    )
origin_cnt.show(truncate = False)

print(f"Compute Year, Month, Date")
purchase_df = cars_df.select(col("Origin"), col("Name"), col("Weight_in_lbs"), col("Year")) \
    .withColumn("Purchasing_Year",substring(col("Year"), 1, 4)) \
    .withColumn("Purchasing_Month", substring(col("Year"),6,2)) \
    .withColumn("Purchasing_Date", substring(col("Year"),9,2)) \

purchase_df.show(truncate=False)
spark.stop()
from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from delta import *
from delta.tables import *
# pip install delta-spark==3.0.0

if __name__ == "__main__":
    print("Package : DeltaLake, Script : Cars Data Analysis 1")

    spark = (
        pyspark.sql.SparkSession.builder.appName("Cars Data Analysis 1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        ).getOrCreate()


cars_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/cars.json"))

print(f"Number of records in the cars_df DF: {cars_df.count()}")

print(f"Number of origins")
origin_cnt = cars_df.select(col("Origin")) \
    .groupBy(col("Origin")) \
    .agg(
      count("Origin")
    ) \
    .show(truncate=False)

print(f"Compute Year & Month")

purchase_df = (cars_df.select(col("Origin"), col("Name"), col("Weight_in_lbs"), col("Year")) \
    .withColumn("Purchasing_Year",substring(col("Year"), 1, 4))
    .withColumn("Purchasing_Month", substring(col("Year"),6,2))
    .withColumn("Purchasing_Date", substring(col("Year"),9,2)))

purchase_df.write.mode("overwrite").format("delta").save("D:/Deltalake/cars_analysis_1")

# print("Validating data from delta")
# validate_df = spark.read.format("delta").load("D:/Deltalake/cars_analysis_1")
# print(f"Delta Read Completed")
# validate_df.show()

spark.stop()
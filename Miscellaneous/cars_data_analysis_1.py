from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Miscellaneous, Script : Cars Data Analysis 1")

spark = SparkSession.builder.appName("Cars Data Analysis 1").master("local[3]").getOrCreate()

cars_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") ## dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") ## bzip2, gzip, lz4, deflate, uncompressed
    .json("D:/DataSet/DataSet/SparkDataSet/cars.json"))

# cars_df.printSchema()
# cars_df.show(10, False)

origin_df = cars_df.select(col("Name"), col("Origin"),
      col("Weight_in_lbs"), col("Year")) \
      .withColumn("Manufacture_Year", substring(col("Year"), 1, 4).cast("Int")) \
      .withColumn("Weight_in_kgs", col("Weight_in_lbs") * 2.2)

origin_df.printSchema()
origin_df.show(10, False)

spark.stop()
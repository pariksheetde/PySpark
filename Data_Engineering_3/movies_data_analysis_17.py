from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 17")

spark = SparkSession.builder.appName("Movies Data Analysis 17").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json"))

null_df = movies_df.fillna(value= 0, subset = ["US_Gross", "US_DVD_Sales"])

release_dt_cln = null_df.select(col("Title"), col("Director"), col("Release_date"),
      col("Major_Genre"), col("US_DVD_Sales"), col("US_Gross")) \
      .withColumn("Date", when(substring(col("Release_Date"), 1, 2).contains ("-"), concat(lit("0"), substring(col("Release_Date"), 0, 1))).otherwise (substring(col("Release_Date"), 1, 2))) \
      .withColumn("Month", substring(col("Release_Date"), -6, 3)) \
      .withColumn("Year", when(substring(col("Release_Date"), -2, 2) < 20, substring(col("Release_Date"), -2, 2) + 2000).otherwise(substring(col("Release_Date"), -2, 2) + 1900)
      ).drop("Release_Date")

movies_release_dt_df = release_dt_cln.select(col("Title"), col("Director"), col("Major_Genre"),
      col("Date"), col("Month"), col("Year"),
      (col("US_DVD_Sales") + col("US_Gross")).alias("Collection")) \
      .withColumn("Date", expr("Date").cast(IntegerType())) \
      .withColumn("Year", expr("Year").cast(IntegerType())) \
      .where("Director is not null")

print(f"calculate collection partitioned by Director")
coll_dir = movies_release_dt_df.select("Director", "Title", "Collection") \
      .withColumn("Rank", rank().over(Window.partitionBy("Director").orderBy("Collection").orderBy(col("Collection").desc())))
coll_dir.show(10, False)

print(f"Calculate highest collection partitioned by Director")
coll_highest_dir = movies_release_dt_df.select("Director", "Title", "Collection") \
      .withColumn("Rank", rank().over(Window.partitionBy("Director").orderBy("Collection").orderBy(col("Collection").desc()))) \
      .filter("Rank = 1")
coll_highest_dir.show(10, False)

print(f"Calculate highest collection partitioned by Director in each Year")
coll_highest_dir_year = movies_release_dt_df.select("Year", "Director", "Title", "Collection") \
      .withColumn("Rank", rank().over(Window.partitionBy("Director", "Year").orderBy(col("Collection").desc()).orderBy(col("Year").asc()))) \
      .filter("Rank = 1 and Director = 'Steven Spielberg'")

coll_highest_dir_year.show(100, False)

spark.stop()
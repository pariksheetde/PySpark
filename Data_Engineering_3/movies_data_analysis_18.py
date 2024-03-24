from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 18")

spark = SparkSession.builder.appName("Movies Data Analysis 18").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json"))

null_chk = movies_df.fillna(value= 0, subset = ["US_Gross", "US_DVD_Sales"])

print(f"Number of Movies {null_chk.count()}")

release_dt_cln = null_chk.select(col("Title"), col("Director"), col("Release_date"),
      col("Major_Genre"), col("US_DVD_Sales"), col("US_Gross")) \
      .withColumn("Date", when(substring(col("Release_Date"), 1, 2).contains ("-"), concat(lit("0"), substring(col("Release_Date"), 0, 1))).otherwise (substring(col("Release_Date"), 1, 2))) \
      .withColumn("Month", substring(col("Release_Date"), -6, 3)) \
      .withColumn("Year", when(substring(col("Release_Date"), -2, 2) < 20, substring(col("Release_Date"), -2, 2) + 2000).otherwise (substring(col("Release_Date"), -2, 2) + 1900) \
      ).drop("Release_Date")

movies_release_dt_df = release_dt_cln.select(col("Title"), col("Director"), col("Major_Genre"),
      col("Date"), col("Month"), col("Year"),
      (col("US_DVD_Sales") + col("US_Gross")).alias("Collection")) \
      .withColumn("Date", col("Date").cast(IntegerType())) \
      .withColumn("Year", col("Year").cast(IntegerType())) \
      .where("Director is not null and Major_Genre is not null")
movies_release_dt_df.show(10, False)

print(f"calculate top earning movies in each year for each director")
top_movies_year_dir = movies_release_dt_df.select("Year", "Major_Genre", "Collection") \
      .withColumn("Rank", rank().over(Window.partitionBy("Year", "Major_Genre").orderBy(col("Year").desc()).orderBy(col("Collection").desc())))
top_movies_year_dir.show(10, False)

spark.stop()
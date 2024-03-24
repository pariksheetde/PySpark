from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 11")

spark = SparkSession.builder.appName("Movies Data Analysis 11").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json")
    .fillna(value="Not Listed", subset= ["Director"])
    .fillna(value= "Not Listed", subset = ["Major_Genre"])
    .fillna(value= 0, subset = ["US_DVD_Sales", "US_Gross", "IMDB_Rating", "Rotten_Tomatoes_Rating"]))

print(f"calculate profit, rating earned for each director for each Genre")
release_dt_cln = movies_df.select(col("Title"), col("Director"), col("Release_date"), col("Major_Genre")) \
      .withColumn("Date", when(substring(col("Release_Date"),1,2).contains("-"), concat(lit("0"),substring(col("Release_Date"),0,1))).otherwise(substring(col("Release_Date"), 1, 2))) \
      .withColumn("Month", substring(col("Release_Date"),-6,3)) \
      .withColumn("Year",when(substring(col("Release_Date"), -2, 2) < 20, substring(col("Release_Date"), -2, 2) + 2000).otherwise(substring(col("Release_Date"),-2, 2) + 1900)
      ).drop("Release_Date")

movies_release_dt_df = release_dt_cln.select(col("Title"), col("Director"), col("Major_Genre"),
      col("Date"), col("Month"), col("Year")) \
      .withColumn("Date", expr("Date").cast(IntegerType())) \
      .withColumn("Year", expr("Year").cast(IntegerType()))

movies_release_dt_df.show(10, False)

print(f"calculate number of movies released per year for each director")

title_year_agg = movies_release_dt_df.select(col("Title"), col("Year"), col("Director")) \
      .groupBy(col("Year"), col("Director")) \
      .agg(
        count(col("Title")).alias("Number_of_Movies")
      ) \
      .where("Director is not null") \
      .sort(col("Number_of_Movies").desc_nulls_last())

title_year_agg.show(10, False)

print(f"calculate number of movies, genre released per year for each director")

title_genre_per_year = movies_release_dt_df.select(col("Title"), col("Director")) \
      .groupBy(col("Director")) \
      .agg(
        count(col("Title")).alias("Number_of_Movies")
      ).sort(col("Director").desc_nulls_last()) \
      .where("Director = 'Steven Spielberg'")

title_genre_per_year.show(10, False)

spark.stop()
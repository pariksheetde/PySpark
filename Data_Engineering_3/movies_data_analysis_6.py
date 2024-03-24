from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 6")

spark = SparkSession.builder.appName("Movies Data Analysis 6").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json")) \
    .fillna(value="No Director", subset= ["Director"]) \
    .fillna(value= "Not Listed", subset = ["Major_Genre"])

movies_df.show(10, False)

print(f"select Title, Director, Top Rating for each Genre")

top_rating = movies_df.select(col("Title"), col("Director"), col("Major_Genre"),
      col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"),
      coalesce(col("Rotten_Tomatoes_Rating") / 10, col("IMDB_Rating")).alias("Rating"))

print(f"calculate top rating for each director in each genre")

top_rating_genre = top_rating.select(col("Director"), col("Major_Genre"), col("Rating")) \
      .groupBy(col("Director"), col("Major_Genre")) \
      .agg(
        max(col("Rating")).alias("Max_Rating"),
        min(col("Rating")).alias("Min_Rating"),
        round(avg(col("Rating")), 2).alias("Avg_Rating")
      ) \
      .sort(col("Director").desc_nulls_last()) \
      # .where("Director in ('Steven Spielberg')") \

top_rating_genre.show(10, False)

spark.stop()
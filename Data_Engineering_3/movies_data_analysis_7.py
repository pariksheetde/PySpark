from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 7")

spark = SparkSession.builder.appName("Movies Data Analysis 7").master("local[3]").getOrCreate()

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
    .fillna(value= 0, subset = ["US_DVD_Sales", "US_Gross"]))

movies_df.show(10, False)

profit_rating_cln_df = movies_df.select(col("Director"), col("Major_Genre"),
    col("US_DVD_Sales"), col("US_Gross"), col("Title"),
    col("IMDB_Rating"), col("Rotten_Tomatoes_Rating"),
    coalesce(col("IMDB_Rating"), col("Rotten_Tomatoes_Rating") / 10).alias("Rating"),
    (col("US_DVD_Sales") + col("US_Gross")).alias("Profit")
  )
profit_rating_cln_df.show(10, False)

print(f"Calculate Total Gross, Number of Movies and Avg(Rating) for each Director, Genre")

profit_rating_agg = profit_rating_cln_df.select(col("Director"), col("Major_Genre"),
      col("US_Gross"), col("Rating"), col("Title"), col("Profit")) \
      .groupBy(col("Director"), col("Major_Genre")) \
      .agg(
        sum("Profit").alias("Sum_Gross"),
        count("Title").alias("Number_of_Movies"),
        round(avg("Rating"), 2).alias("Avg_Rating")
      ) \
      .sort(col("Director").asc_nulls_last()) \
      .where("Director = 'Steven Spielberg'")

profit_rating_agg.show(10, False)

spark.stop()
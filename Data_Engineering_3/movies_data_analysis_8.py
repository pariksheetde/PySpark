from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 8")

spark = SparkSession.builder.appName("Movies Data Analysis 8").master("local[3]").getOrCreate()

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

rating_df = movies_df.select(col("Title"), col("Major_Genre"), col("Director"),
      (coalesce(col("IMDB_Rating"), col("Rotten_Tomatoes_Rating") / 10)).alias("Rating")
    )
rating_df.show(10, False)

# winSpec = Window.partitionBy("Major_Genre").orderBy(col("Lowest_Rating").asc)
lowest_rating = rating_df.select(col("Director"), col("Major_Genre"), col("Rating")) \
      .groupBy("Major_Genre", "Director") \
      .agg(
        round(mean(col("Rating")), 2).alias("Lowest_Rating")
      ) \
    # .withColumn("rank", rank().over(winSpec)) \
    # .where("rank = 1")

lowest_rating.show(10, False)
spark.stop()